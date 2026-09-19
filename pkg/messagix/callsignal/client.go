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

// Package callsignal keeps the Messenger web client's call-signalling
// channels open for one login, so the bridge can be rung and can take part
// in calls:
//
//   - wss://gateway.facebook.com/ws/rpsignaling, a DGW socket with one
//     unified stream (x-dgw-app-useUnifiedStream=true, EstablishStream "{}")
//     carrying Thrift multiway messages (package rtcsignal) in both
//     directions. Every data frame is acked; the socket pings every 10 s.
//   - wss://edge-chat.facebook.com/chat, an MQTT connection subscribed to
//     /t_rtc_multi only. In the callee capture the RING arrived there, and
//     the reply (RingResponse) went out on rpsignaling.
//
// Both channels use the same device id (x-dgw-deviceid = MQTT cid), like the
// web client, which keeps it in local storage: the capture shows the server
// delivering a RING retry on the rpsignaling socket whose device id matched
// the MQTT client id, and not on the one whose id did not.
package callsignal

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/rs/zerolog"
	"go.mau.fi/util/exsync"

	"go.mau.fi/mautrix-meta/pkg/messagix/dgw"
	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
)

// Transport names the channel a message arrived on.
type Transport string

const (
	TransportDGW  Transport = "rpsignaling"
	TransportMQTT Transport = "mqtt"
)

// Handler receives every server-initiated multiway message (requests, not
// responses to our own requests). It runs on the socket's frame goroutine
// and must not block. It returns the response to send, or nil to send the
// web client's default acknowledgement (rtcsignal.DefaultResponseBody).
type Handler func(ctx context.Context, msg *rtcsignal.Message, via Transport) *rtcsignal.Message

// Options configures a Client.
type Options struct {
	GetCookies     func() string
	Origin         string
	RPSignalingURL string
	// MQTTURL enables the /t_rtc_multi listener when non-empty.
	MQTTURL    string
	MQTTRegion string
	DialOpts   websocket.DialOptions
	AppID      string
	UserID     string
	// DeviceID must stay the same across reconnects and restarts.
	DeviceID  string
	UserAgent string
	Log       zerolog.Logger
	Handler   Handler
}

type pendingRequest struct {
	ch   chan *rtcsignal.Message
	hook func(*rtcsignal.Message)
}

// RequestTimeout bounds how long Request waits for the server's reply.
const RequestTimeout = 10 * time.Second

const maxBackoff = 5 * time.Minute

var (
	ErrNotConnected = errors.New("callsignal: rpsignaling stream is not connected")
	ErrStopped      = errors.New("callsignal: client stopped")
)

// Client is the per-login call signalling client.
type Client struct {
	opts   Options
	socket *dgw.Socket
	stream atomic.Pointer[dgw.PersistentStream]
	mqtt   *mqttListener

	// parentSessionID is the clientSessionId used for replies that do not
	// belong to a call context of ours (e.g. RingResponse).
	parentSessionID string
	parentSeq       atomic.Int64

	pendingLock sync.Mutex
	pending     map[string]*pendingRequest

	connected *exsync.Event
	cancel    context.CancelFunc
	stopped   *exsync.Event
	running   atomic.Bool
}

// New creates a client. Call Start to connect.
func New(opts Options) *Client {
	c := &Client{
		opts:            opts,
		parentSessionID: rtcsignal.NewClientSessionID(),
		pending:         make(map[string]*pendingRequest),
		connected:       exsync.NewEvent(),
		stopped:         exsync.NewEvent(),
	}
	c.stopped.Set()
	c.socket = dgw.NewSocket(dgw.SocketOptions{
		GetCookies:     opts.GetCookies,
		OnConnect:      c.onConnect,
		Origin:         opts.Origin,
		WSURL:          opts.RPSignalingURL,
		DialOpts:       opts.DialOpts,
		Log:            opts.Log.With().Str("socket", "rpsignaling").Logger(),
		Facebook:       true,
		LoggingID:      true,
		AppStreamGroup: rtcsignal.RPSignalingStreamGroup,
		AppID:          opts.AppID,
		UserID:         opts.UserID,
		DeviceID:       opts.DeviceID,
		ExtraQuery:     url.Values{"x-dgw-app-useUnifiedStream": {"true"}},
	})
	if opts.MQTTURL != "" {
		c.mqtt = newMQTTListener(c, opts)
	}
	return c
}

// Start connects both channels and keeps reconnecting until Stop.
func (c *Client) Start(ctx context.Context) {
	if !c.running.CompareAndSwap(false, true) {
		return
	}
	ctx, cancel := context.WithCancel(c.opts.Log.WithContext(context.WithoutCancel(ctx)))
	c.cancel = cancel
	c.stopped.Clear()
	go c.connectLoop(ctx)
	if c.mqtt != nil {
		go c.mqtt.loop(ctx)
	}
}

// Stop disconnects and waits briefly for the loops to end.
func (c *Client) Stop() {
	if !c.running.CompareAndSwap(true, false) {
		return
	}
	c.cancel()
	c.socket.Disconnect()
	if c.mqtt != nil {
		c.mqtt.close()
	}
	if !c.stopped.WaitTimeout(5 * time.Second) {
		c.opts.Log.Warn().Msg("Call signalling loop didn't stop in time")
	}
}

// IsConnected reports whether the rpsignaling stream is established.
func (c *Client) IsConnected() bool { return c.stream.Load() != nil }

// WaitConnected blocks until the rpsignaling stream is up or ctx ends.
func (c *Client) WaitConnected(ctx context.Context) error {
	if c.IsConnected() {
		return nil
	}
	select {
	case <-c.connected.GetChan():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) connectLoop(ctx context.Context) {
	defer c.stopped.Set()
	log := zerolog.Ctx(ctx)
	failures := 0
	for {
		start := time.Now()
		err := c.socket.Connect(ctx)
		c.stream.Store(nil)
		c.connected.Clear()
		c.failPending()
		if ctx.Err() != nil {
			log.Debug().AnErr("connect_err", err).Msg("Call signalling socket stopped")
			return
		}
		if websocket.CloseStatus(err) == dgw.CloseStatusUnauthorized {
			log.Err(err).Msg("Call signalling socket unauthorized, not reconnecting")
			return
		}
		if time.Since(start) > 5*time.Minute {
			failures = 0
		}
		failures++
		backoff := min(time.Duration(1<<min(failures, 9))*time.Second, maxBackoff)
		log.Warn().Err(err).Int("failures", failures).Dur("reconnect_in", backoff).
			Msg("Call signalling socket disconnected, reconnecting")
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

func (c *Client) onConnect(ctx context.Context, _ func(error)) error {
	stream, err := c.socket.EstablishStream(ctx, dgw.StreamInit{
		Parameters: []byte(rtcsignal.RPSignalingEstablishParam),
		LogName:    "rpsignaling",
		FrameHandler: func(ctx context.Context, payload []byte) error {
			c.handlePayload(ctx, payload)
			return nil
		},
		OnClose: func() {
			zerolog.Ctx(ctx).Debug().Msg("rpsignaling stream closed by server")
			c.socket.ForceReconnect()
		},
	})
	if err != nil {
		return fmt.Errorf("failed to establish rpsignaling stream: %w", err)
	}
	c.stream.Store(stream)
	c.connected.Set()
	zerolog.Ctx(ctx).Info().Msg("Call signalling stream established")
	return nil
}

func (c *Client) handlePayload(ctx context.Context, payload []byte) {
	log := zerolog.Ctx(ctx)
	msg, err := rtcsignal.DecodePayload(payload, true)
	if err != nil {
		// Never log the payload: it may contain SDP with ICE credentials.
		log.Warn().Err(err).Int("len", len(payload)).Msg("Failed to decode rpsignaling payload")
		return
	}
	c.dispatch(ctx, msg, TransportDGW)
}

// LogMessage adds the non-secret identifying fields of a message to a log
// event.
func LogMessage(e *zerolog.Event, msg *rtcsignal.Message) *zerolog.Event {
	return e.Stringer("msg_type", msg.Header.Type).
		Str("body", msg.Body.Name()).
		Str("txn", msg.Header.TransactionID).
		Int32("status", msg.Header.ResponseStatusCode).
		Int32("sub_code", msg.Header.ResponseSubCode).
		Str("conference", msg.Header.ConferenceName).
		Int64("seq", msg.Header.SequenceNumber).
		Int16("retry", msg.Header.RetryCount)
}

func (c *Client) dispatch(ctx context.Context, msg *rtcsignal.Message, via Transport) {
	log := zerolog.Ctx(ctx)
	LogMessage(log.Debug(), msg).Str("via", string(via)).Msg("Received call signalling message")
	if msg.Header.IsResponse() {
		c.pendingLock.Lock()
		req, ok := c.pending[msg.Header.TransactionID]
		if ok {
			delete(c.pending, msg.Header.TransactionID)
		}
		c.pendingLock.Unlock()
		if ok {
			if req.hook != nil {
				// Runs before the next frame is handled, so state derived
				// from the response (e.g. the conference a JOIN created)
				// is in place for the server pushes that follow it.
				req.hook(msg)
			}
			req.ch <- msg
		} else {
			LogMessage(log.Debug(), msg).Msg("Response to unknown transaction")
		}
		return
	}
	var resp *rtcsignal.Message
	if c.opts.Handler != nil {
		resp = c.opts.Handler(ctx, msg, via)
	}
	if resp == nil {
		resp = c.DefaultResponse(msg)
	}
	go func() {
		if err := c.Send(ctx, resp); err != nil {
			LogMessage(log.Warn().Err(err), resp).Msg("Failed to send response")
		}
	}()
}

// DefaultResponse builds the web client's acknowledgement of a server
// request outside of any call context of ours.
func (c *Client) DefaultResponse(req *rtcsignal.Message) *rtcsignal.Message {
	if req.Body.RingRequest != nil {
		return rtcsignal.NewRingResponse(req, c.parentSessionID, rtcsignal.DeviceStatusOK)
	}
	return rtcsignal.NewResponse(req, c.parentSeq.Add(1)-1, c.opts.UserID, c.parentSessionID, rtcsignal.DefaultResponseBody(req))
}

// Send sends a message and waits for the DGW-level ack only.
func (c *Client) Send(ctx context.Context, msg *rtcsignal.Message) error {
	stream := c.stream.Load()
	if stream == nil {
		return ErrNotConnected
	}
	payload, err := rtcsignal.EncodePayload(msg)
	if err != nil {
		return err
	}
	LogMessage(zerolog.Ctx(ctx).Debug(), msg).Msg("Sending call signalling message")
	return stream.SendData(ctx, payload)
}

// Request sends a request and waits for the server's response with the same
// transaction id. A non-200 status is returned as an error along with the
// response.
func (c *Client) Request(ctx context.Context, msg *rtcsignal.Message) (*rtcsignal.Message, error) {
	return c.RequestHook(ctx, msg, nil)
}

// RequestHook is Request with a hook that runs synchronously on the socket
// goroutine when the response arrives, before any later message is
// dispatched.
func (c *Client) RequestHook(ctx context.Context, msg *rtcsignal.Message, hook func(*rtcsignal.Message)) (*rtcsignal.Message, error) {
	txn := msg.Header.TransactionID
	req := &pendingRequest{ch: make(chan *rtcsignal.Message, 1), hook: hook}
	ch := req.ch
	c.pendingLock.Lock()
	c.pending[txn] = req
	c.pendingLock.Unlock()
	defer func() {
		c.pendingLock.Lock()
		if c.pending[txn] == req {
			delete(c.pending, txn)
		}
		c.pendingLock.Unlock()
	}()
	if err := c.Send(ctx, msg); err != nil {
		return nil, err
	}
	timer := time.NewTimer(RequestTimeout)
	defer timer.Stop()
	select {
	case resp, ok := <-ch:
		if !ok || resp == nil {
			return nil, ErrNotConnected
		}
		if resp.Header.ResponseStatusCode != rtcsignal.StatusOK {
			return resp, fmt.Errorf("callsignal: %s failed with status %d/%d: %s", msg.Header.Type,
				resp.Header.ResponseStatusCode, resp.Header.ResponseSubCode, resp.Header.ResponseStatusMessage)
		}
		return resp, nil
	case <-timer.C:
		return nil, fmt.Errorf("callsignal: %s timed out after %s", msg.Header.Type, RequestTimeout)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Client) failPending() {
	c.pendingLock.Lock()
	defer c.pendingLock.Unlock()
	for txn, req := range c.pending {
		close(req.ch)
		delete(c.pending, txn)
	}
}
