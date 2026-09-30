package connector

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

const liveLocationDescription = "Live location"

// A share Messenger gives no end for lasts this long in Matrix, the longest Messenger offers.
const defaultLiveLocationTimeout = 8 * time.Hour

// liveLocationStartID is the message that starts one live location share: its beacon_info.
func liveLocationStartID(threadKey, sender, startMS int64) networkid.MessageID {
	return networkid.MessageID(fmt.Sprintf("fb.live_location:%d:%d:%d", threadKey, sender, startMS))
}

func liveLocationTimeout(row *table.LSUpsertLiveLocationSharer) time.Duration {
	if row.EndTimestampMS > row.StartTimestampMS {
		return time.Duration(row.EndTimestampMS-row.StartTimestampMS) * time.Millisecond
	}
	return defaultLiveLocationTimeout
}

// liveLocationStartParts starts a Matrix live location: the sharer's beacon_info, then the first position
// as a beacon referring to it.
func liveLocationStartParts(stateKey string, row *table.LSUpsertLiveLocationSharer) []*bridgev2.ConvertedMessagePart {
	start := time.UnixMilli(row.StartTimestampMS)
	return []*bridgev2.ConvertedMessagePart{{
		Type:     event.StateUnstableBeaconInfo,
		StateKey: &stateKey,
		Content: &event.MessageEventContent{
			MsgType: event.MsgLocation,
			Body:    liveLocationDescription,
			GeoURI:  event.GeoURI(row.Latitude, row.Longitude, 0),
		},
		Extra: event.BeaconInfoContent(liveLocationDescription, true, start, liveLocationTimeout(row)),
	}, {
		ID:                 "beacon",
		Type:               event.EventUnstableBeacon,
		ReferencesPrevious: true,
		Extra:              event.BeaconContent("", event.GeoURI(row.Latitude, row.Longitude, 0), start),
	}}
}

// liveLocationUpdatePart is a later position of a share: a beacon referring to its beacon_info.
func liveLocationUpdatePart(beaconInfo id.EventID, row *table.LSUpsertLiveLocationSharer, ts time.Time) *bridgev2.ConvertedMessagePart {
	return &bridgev2.ConvertedMessagePart{
		Type:  event.EventUnstableBeacon,
		Extra: event.BeaconContent(beaconInfo, event.GeoURI(row.Latitude, row.Longitude, 0), ts),
	}
}

func (m *MetaClient) handleLiveLocationUpsert(tk handlerParams, row *table.LSUpsertLiveLocationSharer) bridgev2.RemoteEvent {
	startID := liveLocationStartID(tk.ID, row.Sender, row.StartTimestampMS)
	info, err := m.Main.Bridge.DB.Message.GetFirstPartByID(tk.ctx, m.UserLogin.ID, startID)
	if err != nil {
		zerolog.Ctx(tk.ctx).Err(err).Msg("Failed to look up live location")
		return nil
	}
	now := time.Now()
	evt := &simplevent.Message[*table.LSUpsertLiveLocationSharer]{
		EventMeta: simplevent.EventMeta{
			Type:              bridgev2.RemoteEventMessage,
			PortalKey:         tk.Portal,
			UncertainReceiver: tk.IsUncertainReceiver(),
			Sender:            m.makeEventSender(row.Sender),
			Timestamp:         now,
			LogContext: func(c zerolog.Context) zerolog.Context {
				return c.Str("live_location", string(startID))
			},
		},
		Data: row,
	}
	if info == nil {
		evt.ID = startID
		evt.Timestamp = time.UnixMilli(row.StartTimestampMS)
		evt.ConvertMessageFunc = func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, row *table.LSUpsertLiveLocationSharer) (*bridgev2.ConvertedMessage, error) {
			return &bridgev2.ConvertedMessage{Parts: liveLocationStartParts(intent.GetMXID().String(), row)}, nil
		}
		return evt
	}
	// A later position of a share already bridged: a beacon referring to its beacon_info.
	evt.ID = networkid.MessageID(fmt.Sprintf("%s:%d", startID, now.UnixMilli()))
	evt.ConvertMessageFunc = func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, row *table.LSUpsertLiveLocationSharer) (*bridgev2.ConvertedMessage, error) {
		return &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{liveLocationUpdatePart(info.MXID, row, now)}}, nil
	}
	return evt
}

// handleLiveLocationDelete ends a share: the sharer's beacon_info (keyed by them, so the start isn't needed)
// is set to not live.
func (m *MetaClient) handleLiveLocationDelete(tk handlerParams, row *table.LSDeleteLiveLocationSharer) bridgev2.RemoteEvent {
	now := time.Now()
	return &simplevent.Message[*table.LSDeleteLiveLocationSharer]{
		EventMeta: simplevent.EventMeta{
			Type:              bridgev2.RemoteEventMessage,
			PortalKey:         tk.Portal,
			UncertainReceiver: tk.IsUncertainReceiver(),
			Sender:            m.makeEventSender(row.Sender),
			Timestamp:         now,
		},
		ID:   networkid.MessageID(fmt.Sprintf("fb.live_location:%d:%d:stop:%d", tk.ID, row.Sender, now.UnixMilli())),
		Data: row,
		ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, row *table.LSDeleteLiveLocationSharer) (*bridgev2.ConvertedMessage, error) {
			stateKey := intent.GetMXID().String()
			return &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
				Type:     event.StateUnstableBeaconInfo,
				StateKey: &stateKey,
				Content:  &event.MessageEventContent{MsgType: event.MsgNotice, Body: "Stopped sharing live location"},
				Extra:    event.BeaconInfoContent(liveLocationDescription, false, now, 0),
			}}}, nil
		},
	}
}
