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

package connector

// Call notices (stage (a) of call bridging): post "incoming call / missed
// call / call ended" lines into the portal. Calls are not answered here.
//
// Two channels report calls:
//   - the E2EE (WA-protocol) socket: <notification type="fb:call"><call_event
//     event_type="started|ended" .../></notification>, with the caller, the
//     duration and the call handle (server_info_data). Authoritative.
//   - Lightspeed: updateThreadOngoingCallState / updateOrInsertRtcOngoingCallData
//     / deleteRtcOngoingCallData. Their argument layouts are inferred (see
//     table/calls.go), they carry no caller or duration, and in E2EE threads
//     they arrive just before the fb:call notification. LS observations are
//     therefore delayed by callLSGrace so the richer WA event wins, and only
//     used on their own when no WA event came (non-E2EE threads).
//
// callTracker merges both into one start and one end notice per call, keyed by
// portal and call handle.

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	waTypes "go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

const (
	// callLSGrace delays Lightspeed call observations so that the fb:call
	// notification for the same call (which has the caller and duration) is
	// processed first.
	callLSGrace = 4 * time.Second
	// callEndedWindow is how long after an end further events for the same
	// portal without a distinguishing call id are treated as the same call.
	callEndedWindow = 30 * time.Second
	// callForgetAfter drops finished calls from memory.
	callForgetAfter = 10 * time.Minute
)

type callSource int

const (
	callSourceWA callSource = iota
	callSourceLS
)

type callEventKind int

const (
	callStarted callEventKind = iota
	callEnded
)

// callEvent is one observation of a call from either channel.
type callEvent struct {
	Source      callSource
	Kind        callEventKind
	Portal      networkid.PortalKey
	CallID      string // server_info_data; "" if unknown
	Actor       int64  // user who started/ended the call; 0 if unknown
	Video       bool
	VideoKnown  bool
	Duration    time.Duration
	HasDuration bool
	Time        time.Time
}

type callNoticeKind int

const (
	noticeIncoming    callNoticeKind = iota // someone else started a call
	noticeOutgoing                          // the user started a call on another device
	noticeOngoing                           // a call started, direction unknown (LS only)
	noticeMissed                            // incoming, never answered
	noticeNotAnswered                       // outgoing, never answered
	noticeEnded                             // answered call ended (duration known if HasDuration)
)

// callNotice is a message to post in the portal.
type callNotice struct {
	Kind        callNoticeKind
	Portal      networkid.PortalKey
	Key         string // stable per call, used for message ids
	Sender      int64  // 0 = bridge bot
	Video       bool
	Duration    time.Duration
	HasDuration bool
	Time        time.Time
}

func (n *callNotice) callType() string {
	if n.Video {
		return "video"
	}
	return "voice"
}

// formatCallDuration renders 74s as "1:14" and 3725s as "1:02:05".
func formatCallDuration(d time.Duration) string {
	s := int64(d.Round(time.Second) / time.Second)
	if s >= 3600 {
		return fmt.Sprintf("%d:%02d:%02d", s/3600, s/60%60, s%60)
	}
	return fmt.Sprintf("%d:%02d", s/60, s%60)
}

func (n *callNotice) Text() string {
	ct := n.callType()
	switch n.Kind {
	case noticeIncoming:
		return fmt.Sprintf("📞 Incoming %s call — answer on Messenger", ct)
	case noticeOutgoing:
		return fmt.Sprintf("📞 Outgoing %s call", ct)
	case noticeOngoing:
		return fmt.Sprintf("📞 %s call started — join on Messenger", capitalize(ct))
	case noticeMissed:
		return fmt.Sprintf("Missed %s call", ct)
	case noticeNotAnswered:
		return fmt.Sprintf("%s call not answered", capitalize(ct))
	default:
		if n.HasDuration && n.Duration > 0 {
			return fmt.Sprintf("Call ended (%s)", formatCallDuration(n.Duration))
		}
		return "Call ended"
	}
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return string(s[0]-'a'+'A') + s[1:]
}

func (n *callNotice) IsEnd() bool { return n.Kind >= noticeMissed }

func (n *callNotice) MessageID() networkid.MessageID {
	suffix := "start"
	if n.IsEnd() {
		suffix = "end"
	}
	return networkid.MessageID("fbcall:" + n.Key + ":" + suffix)
}

func (n *callNotice) Content() (*event.MessageEventContent, map[string]any) {
	action := map[string]any{"type": "call", "call_type": n.callType()}
	switch n.Kind {
	case noticeIncoming, noticeOutgoing, noticeOngoing:
		action["call_state"] = "started"
	case noticeMissed, noticeNotAnswered:
		action["call_state"] = "missed"
	default:
		action["call_state"] = "ended"
		if n.HasDuration {
			action["duration"] = int64(n.Duration / time.Second)
		}
	}
	return &event.MessageEventContent{MsgType: event.MsgNotice, Body: n.Text()},
		map[string]any{"com.beeper.action_message": action}
}

type trackedCall struct {
	id           string
	key          string
	initiator    int64
	video        bool
	startNoticed bool
	ended        bool
	firstSeen    time.Time
	endTime      time.Time
}

// callTracker deduplicates call observations into notices. It is safe for
// concurrent use and has no side effects besides its own state.
type callTracker struct {
	lock  sync.Mutex
	self  int64
	calls map[networkid.PortalKey]*trackedCall
}

func newCallTracker(self int64) *callTracker {
	return &callTracker{self: self, calls: make(map[networkid.PortalKey]*trackedCall)}
}

func (t *callTracker) Observe(evt *callEvent) *callNotice {
	t.lock.Lock()
	defer t.lock.Unlock()
	for pk, c := range t.calls {
		if c.ended && evt.Time.Sub(c.endTime) > callForgetAfter {
			delete(t.calls, pk)
		}
	}

	c := t.calls[evt.Portal]
	if c != nil {
		differentID := c.id != "" && evt.CallID != "" && c.id != evt.CallID
		staleEnd := c.ended && evt.Time.Sub(c.endTime) > callEndedWindow
		if differentID || staleEnd {
			c = nil
		}
	}
	if c == nil {
		c = &trackedCall{id: evt.CallID, firstSeen: evt.Time}
		t.calls[evt.Portal] = c
	}
	if c.id == "" {
		c.id = evt.CallID
	}
	if c.initiator == 0 && evt.Kind == callStarted && evt.Actor != 0 {
		c.initiator = evt.Actor
	}
	if evt.VideoKnown {
		c.video = c.video || evt.Video
	}
	if c.key == "" {
		if c.id != "" {
			c.key = c.id
		} else {
			c.key = string(evt.Portal.ID) + ":" + strconv.FormatInt(c.firstSeen.UnixMilli(), 10)
		}
	}

	notice := &callNotice{Portal: evt.Portal, Key: c.key, Video: c.video, Time: evt.Time}
	switch evt.Kind {
	case callStarted:
		if c.ended || c.startNoticed {
			return nil
		}
		c.startNoticed = true
		notice.Sender = c.initiator
		switch c.initiator {
		case 0:
			notice.Kind = noticeOngoing
		case t.self:
			notice.Kind = noticeOutgoing
		default:
			notice.Kind = noticeIncoming
		}
	case callEnded:
		if c.ended {
			return nil
		}
		c.ended = true
		c.endTime = evt.Time
		notice.Sender = c.initiator
		notice.Duration, notice.HasDuration = evt.Duration, evt.HasDuration
		answered := evt.HasDuration && evt.Duration > 0
		switch {
		case !evt.HasDuration || c.initiator == 0:
			notice.Kind = noticeEnded
		case answered:
			notice.Kind = noticeEnded
		case c.initiator == t.self:
			notice.Kind = noticeNotAnswered
		default:
			notice.Kind = noticeMissed
		}
	}
	return notice
}

// callAttrString reads an attribute that may have been decoded as a string,
// a JID or a number.
func callAttrString(attrs waBinary.Attrs, key string) string {
	switch v := attrs[key].(type) {
	case string:
		return v
	case waTypes.JID:
		return v.String()
	case int, int64, uint64, int32:
		return fmt.Sprint(v)
	default:
		return ""
	}
}

func callAttrJID(attrs waBinary.Attrs, key string) waTypes.JID {
	switch v := attrs[key].(type) {
	case waTypes.JID:
		return v
	case string:
		jid, err := waTypes.ParseJID(v)
		if err == nil {
			return jid
		}
	}
	return waTypes.EmptyJID
}

// parseFBCallNotification converts <notification type="fb:call"> into a call
// event. The portal is left for the caller to fill in from the returned chat.
func parseFBCallNotification(node *waBinary.Node) (*callEvent, waTypes.JID, error) {
	if node.Tag != "notification" || callAttrString(node.Attrs, "type") != "fb:call" {
		return nil, waTypes.EmptyJID, fmt.Errorf("not an fb:call notification")
	}
	child, ok := node.GetOptionalChildByTag("call_event")
	if !ok {
		return nil, waTypes.EmptyJID, fmt.Errorf("fb:call notification without call_event")
	}
	attrs := child.Attrs
	evt := &callEvent{Source: callSourceWA, CallID: callAttrString(attrs, "server_info_data")}
	switch et := callAttrString(attrs, "event_type"); et {
	case "started":
		evt.Kind = callStarted
	case "ended", "missed":
		evt.Kind = callEnded
	default:
		return nil, waTypes.EmptyJID, fmt.Errorf("unknown call event_type %q", et)
	}
	missed := callAttrString(attrs, "event_type") == "missed"
	switch callAttrString(attrs, "call_type") {
	case "video":
		evt.Video, evt.VideoKnown = true, true
	case "voice", "audio":
		evt.VideoKnown = true
	}
	if actor := callAttrJID(attrs, "event_actor_id"); !actor.IsEmpty() {
		evt.Actor = int64(actor.UserInt())
	}
	if dur := callAttrString(attrs, "duration"); dur != "" {
		if secs, err := strconv.ParseInt(dur, 10, 64); err == nil && secs >= 0 {
			evt.Duration, evt.HasDuration = time.Duration(secs)*time.Second, true
		}
	}
	if missed {
		// Nobody answered: an ended call of zero length (missed / not answered notice).
		evt.Duration, evt.HasDuration = 0, true
	}
	evt.Time = parseCallEventTime(callAttrString(attrs, "event_time"))
	if evt.Time.IsZero() {
		evt.Time = parseCallEventTime(callAttrString(node.Attrs, "t"))
	}
	if evt.Time.IsZero() {
		evt.Time = time.Now()
	}
	chat := callAttrJID(attrs, "jid")
	if chat.IsEmpty() {
		chat = callAttrJID(node.Attrs, "from")
	}
	if chat.IsEmpty() {
		return nil, waTypes.EmptyJID, fmt.Errorf("fb:call notification without chat jid")
	}
	return evt, chat, nil
}

// parseCallEventTime accepts seconds or milliseconds since the epoch.
func parseCallEventTime(s string) time.Time {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 {
		return time.Time{}
	}
	if v > 1e11 {
		return time.UnixMilli(v)
	}
	return time.Unix(v, 0)
}

// findServerInfoData scans the unrecognized values of an LS row for a string
// that decodes as a call handle, since its position is unknown.
func findServerInfoData(values map[int]any) string {
	best := -1
	var found string
	for idx, v := range values {
		s, ok := v.(string)
		if !ok || len(s) < 16 {
			continue
		}
		if d, err := rtcsignal.ParseServerInfoData(s); err == nil && d.CallKey != "" && (best < 0 || idx < best) {
			best, found = idx, s
		}
	}
	return found
}

// lsCallEvents extracts call observations from a Lightspeed table. Rows of
// one table for the same thread and kind are merged into a single event (a
// call start typically comes as updateThreadOngoingCallState, which has the
// media type, plus updateOrInsertRtcOngoingCallData, which has the handle).
func lsCallEvents(tbl *table.LSTable, now time.Time) (events []*callEvent, threadKeys []int64) {
	type key struct {
		thread int64
		kind   callEventKind
	}
	merged := map[key]*callEvent{}
	get := func(thread int64, kind callEventKind) *callEvent {
		k := key{thread, kind}
		if evt, ok := merged[k]; ok {
			return evt
		}
		evt := &callEvent{Source: callSourceLS, Kind: kind, Time: now}
		merged[k] = evt
		events = append(events, evt)
		threadKeys = append(threadKeys, thread)
		return evt
	}
	for _, row := range tbl.LSUpdateThreadOngoingCallState {
		if !row.OngoingCallState.Valid() {
			// The index-1 guess doesn't hold for this row; don't guess further.
			continue
		}
		if row.OngoingCallState == table.RtcCallStateNone {
			get(row.ThreadKey, callEnded)
		} else {
			evt := get(row.ThreadKey, callStarted)
			evt.Video, evt.VideoKnown = row.OngoingCallState.IsVideo(), true
		}
	}
	for _, row := range tbl.LSUpdateOrInsertRtcOngoingCallData {
		evt := get(row.ThreadKey, callStarted)
		if id := findServerInfoData(row.Unrecognized); id != "" {
			evt.CallID = id
		}
	}
	for _, row := range tbl.LSDeleteRtcOngoingCallData {
		get(row.ThreadKey, callEnded)
	}
	return
}

// --- MetaClient wiring ---

func (m *MetaClient) callNoticesEnabled() bool {
	return m.Main.Config.CallNotices && m.calls != nil
}

// handleE2EENode is the messagix E2EENodeTap; it runs on the websocket read
// loop, so the real work happens in a goroutine.
func (m *MetaClient) handleE2EENode(node *waBinary.Node) {
	if node.Tag != "notification" || (!m.callNoticesEnabled() && !m.callBridgingEnabled()) {
		return
	}
	if typ, _ := node.Attrs["type"].(string); typ != "fb:call" {
		return
	}
	go m.handleFBCallNotification(node)
}

func (m *MetaClient) handleFBCallNotification(node *waBinary.Node) {
	log := m.UserLogin.Log.With().Str("action", "handle fb:call notification").Logger()
	evt, chat, err := parseFBCallNotification(node)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to parse call notification")
		return
	}
	evt.Portal = m.makeWAPortalKey(chat)
	if evt.Kind == callEnded {
		m.handleFBCallEnded(evt.CallID)
	}
	log.Debug().
		Int("kind", int(evt.Kind)).
		Int64("actor", evt.Actor).
		Bool("has_call_id", evt.CallID != "").
		Dur("duration", evt.Duration).
		Msg("Received Messenger call notification")
	if m.callNoticesEnabled() {
		m.queueCallNotice(m.calls.Observe(evt))
	}
}

// handleTableCalls feeds the LS call SPs to the tracker after callLSGrace.
func (m *MetaClient) handleTableCalls(ctx context.Context, tbl *table.LSTable, portalFor func(threadKey int64) networkid.PortalKey) {
	if !m.callNoticesEnabled() {
		return
	}
	events, threadKeys := lsCallEvents(tbl, time.Now())
	for i, evt := range events {
		evt.Portal = portalFor(threadKeys[i])
		m.UserLogin.Log.Debug().
			Int64("thread_key", threadKeys[i]).
			Int("kind", int(evt.Kind)).
			Bool("has_call_id", evt.CallID != "").
			Msg("Received Lightspeed call state")
		time.AfterFunc(callLSGrace, func() {
			evt.Time = time.Now()
			m.queueCallNotice(m.calls.Observe(evt))
		})
	}
}

func (m *MetaClient) queueCallNotice(n *callNotice) {
	if n == nil {
		return
	}
	sender := bridgev2.EventSender{}
	if n.Sender != 0 {
		sender = m.makeEventSender(n.Sender)
	}
	m.UserLogin.QueueRemoteEvent(&simplevent.Message[*callNotice]{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventMessage,
			PortalKey: n.Portal,
			Sender:    sender,
			Timestamp: n.Time,
		},
		ID:   n.MessageID(),
		Data: n,
		ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, n *callNotice) (*bridgev2.ConvertedMessage, error) {
			content, extra := n.Content()
			return &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
				Type:    event.EventMessage,
				Content: content,
				Extra:   extra,
			}}}, nil
		},
	})
}
