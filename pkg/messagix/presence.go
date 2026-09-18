// mautrix-meta - A Matrix-Facebook Messenger and Instagram DM puppeting bridge.
// Copyright (C) 2026 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package messagix

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"go.mau.fi/util/exsync"

	"go.mau.fi/mautrix-meta/pkg/instameow/thrift"
	"go.mau.fi/mautrix-meta/pkg/instameow/thrift/requeststream"
	"go.mau.fi/mautrix-meta/pkg/messagix/dgw"
	"go.mau.fi/mautrix-meta/pkg/messagix/presencestream"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

// PresenceEvent is emitted for every presence publish from the
// PresenceUnifiedJSON stream.
type PresenceEvent struct {
	*presencestream.Publish
}

// PresenceStreamClosedEvent is emitted when the presence stream goes away.
// Online states received from it can no longer be trusted after this.
type PresenceStreamClosedEvent struct {
	Err error
}

// MaxPresenceContacts caps the number of additional contacts requested.
const MaxPresenceContacts = 200

const (
	presenceAmendMinInterval = 10 * time.Second
	presenceMaxBackoff       = 30 * time.Minute
	presenceRawLogLimit      = 5
)

var ErrPresenceUnsupported = errors.New("presence stream is not supported on this platform")

type presenceClient struct {
	socket  *dgw.Socket
	stream  atomic.Pointer[dgw.PersistentStream]
	stopped *exsync.Event
	cancel  context.CancelFunc

	lock          sync.Mutex
	contacts      []string
	contactsDirty bool
	lastAmend     time.Time
	nextAmendID   int64
	rawLogged     int
}

func (c *Client) presenceAppFamily() presencestream.AppFamily {
	if c.Platform.IsViaMessenger() {
		return presencestream.AppFamilyMessenger
	}
	return presencestream.AppFamilyFacebook
}

// StartPresenceStream connects to the streamcontroller DGW endpoint and
// subscribes to contact presence. It keeps reconnecting until
// StopPresenceStream or Disconnect is called. Events are delivered through
// the normal event handler as *PresenceEvent and *PresenceStreamClosedEvent.
func (c *Client) StartPresenceStream(ctx context.Context) error {
	if c == nil {
		return ErrClientIsNil
	} else if !c.Platform.IsMessenger() || c.Platform == types.FacebookTor {
		return ErrPresenceUnsupported
	} else if _, ok := c.endpoints["dgw_streamcontroller"]; !ok {
		return ErrPresenceUnsupported
	} else if !c.IsAuthenticated() {
		return fmt.Errorf("messagix-client: not yet authenticated")
	}
	c.presenceLock.Lock()
	defer c.presenceLock.Unlock()
	if c.presence != nil {
		return nil
	}
	log := c.Logger.With().Str("socket", "presence").Logger()
	ctx, cancel := context.WithCancel(log.WithContext(context.WithoutCancel(ctx)))
	pc := &presenceClient{
		stopped: exsync.NewEvent(),
		cancel:  cancel,
	}
	pc.socket = dgw.NewSocket(dgw.SocketOptions{
		GetCookies: func() string {
			return c.GetCookies().String()
		},
		OnConnect: func(ctx context.Context, _ func(error)) error {
			return c.onPresenceSocketConnect(ctx, pc)
		},
		Origin:         c.GetEndpoint("base_url"),
		WSURL:          c.GetEndpoint("dgw_streamcontroller"),
		DialOpts:       *c.http.GetWebsocketDialer(),
		Log:            log,
		Facebook:       true,
		LoggingID:      true,
		AppStreamGroup: "group1",
		AppID:          c.socket.AppID,
		UserID:         c.socket.UserID,
		// The web client generates a new device ID for every stream group.
		DeviceID: uuid.NewString(),
	})
	c.presence = pc
	go c.presenceConnectLoop(ctx, pc)
	go c.presencePollLoop(ctx, pc)
	return nil
}

// StopPresenceStream disconnects the presence stream if it's running.
func (c *Client) StopPresenceStream() {
	if c == nil {
		return
	}
	c.presenceLock.Lock()
	pc := c.presence
	c.presence = nil
	c.presenceLock.Unlock()
	if pc == nil {
		return
	}
	pc.cancel()
	pc.socket.Disconnect()
	if !pc.stopped.WaitTimeout(5 * time.Second) {
		c.Logger.Warn().Msg("Presence stream loop didn't stop in time")
	}
}

// SetPresenceContacts sets the users whose presence should be requested in
// addition to the server-side buddy list, like the web client does for the
// participants of the chats it shows. The list is sent to the server
// rate-limited, and re-sent periodically.
func (c *Client) SetPresenceContacts(userIDs []int64) {
	if c == nil {
		return
	}
	ids := make([]string, 0, min(len(userIDs), MaxPresenceContacts))
	for _, id := range userIDs {
		if id <= 0 {
			continue
		}
		str := strconv.FormatInt(id, 10)
		if !slices.Contains(ids, str) {
			ids = append(ids, str)
		}
		if len(ids) >= MaxPresenceContacts {
			break
		}
	}
	c.presenceLock.Lock()
	c.presenceContacts = ids
	pc := c.presence
	c.presenceLock.Unlock()
	if pc != nil {
		pc.setContacts(ids)
	}
}

func (pc *presenceClient) setContacts(ids []string) {
	pc.lock.Lock()
	defer pc.lock.Unlock()
	if slices.Equal(pc.contacts, ids) {
		return
	}
	pc.contacts = ids
	pc.contactsDirty = true
}

func (c *Client) presenceConnectLoop(ctx context.Context, pc *presenceClient) {
	defer pc.stopped.Set()
	log := zerolog.Ctx(ctx)
	sequentialFailures := 0
	for {
		c.presenceLock.Lock()
		pc.setContacts(c.presenceContacts)
		c.presenceLock.Unlock()
		connectStart := time.Now()
		err := pc.socket.Connect(ctx)
		pc.stream.Store(nil)
		if ctx.Err() != nil {
			log.Debug().AnErr("connect_err", err).Msg("Presence stream stopped")
			c.HandleEvent(ctx, &PresenceStreamClosedEvent{Err: ctx.Err()})
			return
		}
		c.HandleEvent(ctx, &PresenceStreamClosedEvent{Err: err})
		if websocket.CloseStatus(err) == dgw.CloseStatusUnauthorized {
			log.Err(err).Msg("Presence stream unauthorized, not reconnecting")
			return
		}
		if time.Since(connectStart) > 5*time.Minute {
			sequentialFailures = 0
		}
		sequentialFailures++
		backoff := min(time.Duration(1<<min(sequentialFailures, 12))*time.Second, presenceMaxBackoff)
		log.Warn().Err(err).
			Int("sequential_failures", sequentialFailures).
			Dur("reconnect_in", backoff).
			Msg("Presence stream disconnected, reconnecting")
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

func (c *Client) onPresenceSocketConnect(ctx context.Context, pc *presenceClient) error {
	params, err := presencestream.MakeStreamParameters(c.GetEndpoint("messages") + "/")
	if err != nil {
		return fmt.Errorf("failed to marshal presence stream parameters: %w", err)
	}
	init, err := presencestream.MakeInitPayload(&presencestream.Request{
		AppFamily:   c.presenceAppFamily(),
		PollingMode: presencestream.PollingModeBuddyList,
		AppID:       c.configs.BrowserConfigTable.CurrentUserInitialData.AppID,
		// Report ourselves as idle, which is what a web client in a
		// background tab does. The bridge shouldn't make the user look active.
		PresenceReportingRequest: &presencestream.ReportingRequest{
			Capabilities: presencestream.DefaultCapabilities,
			MutationID:   uuid.NewString(),
			Availability: presencestream.AvailabilityIdle,
		},
		PublishEncoding: presencestream.PublishEncodingJSON,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal presence stream request: %w", err)
	}
	stream, err := pc.socket.EstablishStream(ctx, dgw.StreamInit{
		Parameters:  params,
		InitPayload: init,
		LogName:     presencestream.Method,
		FrameHandler: func(ctx context.Context, frame []byte) error {
			return c.handlePresenceFrame(ctx, pc, frame)
		},
		OnClose: func() {
			zerolog.Ctx(ctx).Debug().Msg("Presence stream closed by server")
			pc.socket.ForceReconnect()
		},
	})
	if err != nil {
		return fmt.Errorf("failed to establish presence stream: %w", err)
	}
	pc.stream.Store(stream)
	pc.lock.Lock()
	pc.contactsDirty = len(pc.contacts) > 0
	pc.lastAmend = time.Time{}
	pc.lock.Unlock()
	zerolog.Ctx(ctx).Debug().Msg("Presence stream established")
	return nil
}

var errPresenceTerminated = errors.New("presence stream terminated by server")

func (c *Client) handlePresenceFrame(ctx context.Context, pc *presenceClient, frame []byte) error {
	log := zerolog.Ctx(ctx)
	var payload requeststream.Payload
	if err := thrift.Unmarshal(frame, &payload); err != nil {
		log.Warn().Err(err).Hex("frame", frame).Msg("Failed to unmarshal presence stream frame")
		return nil
	}
	if payload.Response == nil {
		log.Trace().Any("payload", &payload).Msg("Non-response presence stream frame")
		return nil
	}
	var terminated error
	for _, delta := range payload.Response.Delta {
		switch {
		case delta.FlowStatus != nil:
			log.Debug().Stringer("flow_status", *delta.FlowStatus).Msg("Presence stream flow status")
		case delta.Log != nil:
			log.Debug().Str("message", delta.Log.Message).Msg("Presence stream log message")
		case delta.AmendAck != nil:
			log.Debug().
				Any("amendment_id", delta.AmendAck.AmendmentID).
				Any("accepted", delta.AmendAck.Accepted).
				Msg("Presence stream amendment ack")
		case delta.Termination != nil:
			log.Warn().
				Stringer("reason", delta.Termination.Reason).
				Any("message", delta.Termination.Message).
				Any("retry_delay_ms", delta.Termination.RetryDelayMs).
				Msg("Presence stream terminated by server")
			terminated = errPresenceTerminated
		case delta.Data != nil:
			pc.lock.Lock()
			logRaw := pc.rawLogged < presenceRawLogLimit
			pc.rawLogged++
			pc.lock.Unlock()
			if logRaw {
				// Temporary: the first few publishes are logged verbatim to
				// confirm the payload format against live traffic.
				log.Debug().Bytes("data", delta.Data.Bytes).Msg("Raw presence publish")
			}
			pub, err := presencestream.ParsePublish(delta.Data.Bytes)
			if err != nil {
				log.Warn().Err(err).Bytes("data", delta.Data.Bytes).Msg("Failed to parse presence publish")
				continue
			}
			log.Trace().Bytes("data", delta.Data.Bytes).Msg("Presence publish")
			log.Debug().
				Int("publish_type", int(pub.PublishType)).
				Int("update_count", len(pub.PresenceUpdates)).
				Msg("Received presence publish")
			c.HandleEvent(ctx, &PresenceEvent{Publish: pub})
		case delta.Rewrite != nil:
			log.Debug().Any("rewrite", delta.Rewrite).Msg("Presence stream rewrite request (ignored)")
		}
	}
	if payload.Response.GetAckLevel() == requeststream.AckLevel_Device && payload.Response.ResponseID != nil {
		ack, err := presencestream.MakeResponseAck(*payload.Response.ResponseID)
		if err == nil {
			if stream := pc.stream.Load(); stream != nil {
				go func() {
					if err := stream.SendDataNoAck(ctx, ack); err != nil {
						log.Debug().Err(err).Msg("Failed to ack presence stream response")
					}
				}()
			}
		}
	}
	return terminated
}

func (c *Client) presencePollLoop(ctx context.Context, pc *presenceClient) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.maybeSendPresenceContacts(ctx, pc)
		}
	}
}

func (c *Client) maybeSendPresenceContacts(ctx context.Context, pc *presenceClient) {
	stream := pc.stream.Load()
	if stream == nil {
		return
	}
	now := time.Now()
	pc.lock.Lock()
	sinceLast := now.Sub(pc.lastAmend)
	due := len(pc.contacts) > 0 &&
		((pc.contactsDirty && sinceLast >= presenceAmendMinInterval) || sinceLast >= presencestream.PollInterval)
	if !due {
		pc.lock.Unlock()
		return
	}
	contacts := pc.contacts
	pc.nextAmendID++
	amendID := pc.nextAmendID
	pc.contactsDirty = false
	pc.lastAmend = now
	pc.lock.Unlock()
	payload, err := presencestream.MakeAdditionalContactsAmendment(amendID, contacts)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to marshal presence contacts amendment")
		return
	}
	if err = stream.SendData(ctx, payload); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Failed to send presence contacts amendment")
		pc.lock.Lock()
		pc.contactsDirty = true
		pc.lock.Unlock()
		return
	}
	zerolog.Ctx(ctx).Debug().
		Int64("amendment_id", amendID).
		Int("contact_count", len(contacts)).
		Msg("Requested presence of additional contacts")
}
