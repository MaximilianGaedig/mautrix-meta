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

import (
	"strconv"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	waTypes "go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// Synthetic ids; the structure follows a real capture (messenger-call-capture-1.log).
const (
	testSelf = int64(100000000000001)
	testPeer = int64(100000000000002)
)

var (
	testCallID  = (&rtcsignal.ServerInfoData{Region: "abc", CallKey: "0123456789abcdef", Number: 42}).String()
	testCallID2 = (&rtcsignal.ServerInfoData{Region: "abc", CallKey: "fedcba9876543210", Number: 43}).String()
	testPortal  = networkid.PortalKey{ID: "100000000000002", Receiver: "100000000000001"}
	testT0      = time.Date(2026, 9, 18, 22, 35, 43, 0, time.UTC)
)

func msgrJID(id int64) waTypes.JID {
	return waTypes.NewJID(fmtInt(id), waTypes.MessengerServer)
}

func fmtInt(i int64) string { return strconv.FormatInt(i, 10) }

// fbCallNode builds <notification type="fb:call"><call_event .../></notification>
// like the one whatsmeow logs. JIDs are decoded as types.JID and the rest as
// strings, as the binary decoder does.
func fbCallNode(eventType, callType string, actor int64, duration, callID string, eventTime time.Time) *waBinary.Node {
	return &waBinary.Node{
		Tag: "notification",
		Attrs: waBinary.Attrs{
			"from": msgrJID(testPeer),
			"id":   "1234567890",
			"t":    fmtInt(eventTime.Unix()),
			"type": "fb:call",
		},
		Content: []waBinary.Node{{
			Tag: "call_event",
			Attrs: waBinary.Attrs{
				"call_type":        callType,
				"duration":         duration,
				"event_actor_id":   msgrJID(actor),
				"event_time":       fmtInt(eventTime.UnixMilli()),
				"event_type":       eventType,
				"jid":              msgrJID(testPeer),
				"server_info_data": callID,
			},
		}},
	}
}

func parseTestNode(t *testing.T, n *waBinary.Node) *callEvent {
	t.Helper()
	evt, chat, err := parseFBCallNotification(n)
	if err != nil {
		t.Fatal(err)
	}
	if chat.User != fmtInt(testPeer) {
		t.Fatalf("chat jid %s", chat)
	}
	evt.Portal = testPortal
	return evt
}

func TestParseFBCallNotification(t *testing.T) {
	evt := parseTestNode(t, fbCallNode("ended", "voice", testPeer, "74", testCallID, testT0))
	if evt.Kind != callEnded || evt.Actor != testPeer || !evt.HasDuration || evt.Duration != 74*time.Second ||
		evt.CallID != testCallID || evt.Video || !evt.VideoKnown || !evt.Time.Equal(testT0) {
		t.Fatalf("unexpected event: %+v", evt)
	}
	// String-typed JIDs (as when built from XML) work too.
	n := fbCallNode("started", "video", testPeer, "0", testCallID, testT0)
	n.Content.([]waBinary.Node)[0].Attrs["event_actor_id"] = fmtInt(testPeer) + "@msgr"
	if evt = parseTestNode(t, n); evt.Kind != callStarted || !evt.Video || evt.Actor != testPeer {
		t.Fatalf("unexpected event: %+v", evt)
	}
	// "missed" is an ended call nobody answered, whatever duration it carries.
	if evt = parseTestNode(t, fbCallNode("missed", "voice", testPeer, "", testCallID, testT0)); evt.Kind != callEnded ||
		!evt.HasDuration || evt.Duration != 0 {
		t.Fatalf("unexpected missed event: %+v", evt)
	}
	if _, _, err := parseFBCallNotification(&waBinary.Node{Tag: "notification", Attrs: waBinary.Attrs{"type": "fb:call"}}); err == nil {
		t.Fatal("expected error for notification without call_event")
	}
}

func TestCallTrackerIncomingMissed(t *testing.T) {
	tr := newCallTracker(testSelf)
	// LS rows arrive first but are delayed by callLSGrace; WA wins.
	start := tr.Observe(parseTestNode(t, fbCallNode("started", "voice", testPeer, "0", testCallID, testT0)))
	if start == nil || start.Kind != noticeIncoming || start.Sender != testPeer ||
		start.Text() != "📞 Incoming voice call — answer on Messenger" {
		t.Fatalf("start: %+v", start)
	}
	content, extra := start.Content()
	action := extra["com.beeper.action_message"].(map[string]any)
	if content.MsgType != "m.notice" || action["type"] != "call" || action["call_type"] != "voice" {
		t.Fatalf("content: %+v %+v", content, extra)
	}
	lsStart := &callEvent{Source: callSourceLS, Kind: callStarted, Portal: testPortal, CallID: testCallID, Time: testT0.Add(callLSGrace)}
	if n := tr.Observe(lsStart); n != nil {
		t.Fatalf("duplicate start from LS: %+v", n)
	}
	end := tr.Observe(parseTestNode(t, fbCallNode("ended", "voice", testPeer, "0", testCallID, testT0.Add(20*time.Second))))
	if end == nil || end.Kind != noticeMissed || end.Text() != "Missed voice call" || end.MessageID() == start.MessageID() {
		t.Fatalf("end: %+v", end)
	}
	for _, e := range []*callEvent{
		{Source: callSourceLS, Kind: callEnded, Portal: testPortal, Time: testT0.Add(24 * time.Second)},
		{Source: callSourceLS, Kind: callStarted, Portal: testPortal, Time: testT0.Add(24 * time.Second)}, // late LS start
	} {
		if n := tr.Observe(e); n != nil {
			t.Fatalf("duplicate from LS: %+v", n)
		}
	}
}

func TestCallTrackerAnsweredElsewhere(t *testing.T) {
	tr := newCallTracker(testSelf)
	tr.Observe(parseTestNode(t, fbCallNode("started", "voice", testPeer, "0", testCallID, testT0)))
	// The ended notification's actor is whoever hung up; the notice keeps the caller.
	end := tr.Observe(parseTestNode(t, fbCallNode("ended", "voice", testSelf, "74", testCallID, testT0.Add(90*time.Second))))
	if end == nil || end.Kind != noticeEnded || end.Text() != "Call ended (1:14)" || end.Sender != testPeer {
		t.Fatalf("end: %+v", end)
	}
	_, extra := end.Content()
	if d := extra["com.beeper.action_message"].(map[string]any)["duration"]; d != int64(74) {
		t.Fatalf("duration in action message: %v", d)
	}
}

func TestCallTrackerOutgoing(t *testing.T) {
	tr := newCallTracker(testSelf)
	start := tr.Observe(parseTestNode(t, fbCallNode("started", "video", testSelf, "0", testCallID, testT0)))
	if start == nil || start.Kind != noticeOutgoing || start.Text() != "📞 Outgoing video call" {
		t.Fatalf("start: %+v", start)
	}
	end := tr.Observe(parseTestNode(t, fbCallNode("ended", "video", testSelf, "3725", testCallID, testT0.Add(time.Hour))))
	if end == nil || end.Text() != "Call ended (1:02:05)" {
		t.Fatalf("end: %+v", end)
	}
	tr2 := newCallTracker(testSelf)
	tr2.Observe(parseTestNode(t, fbCallNode("started", "voice", testSelf, "0", testCallID, testT0)))
	if end = tr2.Observe(parseTestNode(t, fbCallNode("ended", "voice", testSelf, "0", testCallID, testT0.Add(30*time.Second)))); end.Text() != "Voice call not answered" {
		t.Fatalf("unanswered outgoing: %+v", end)
	}
}

func TestCallTrackerNewCallSamePortal(t *testing.T) {
	tr := newCallTracker(testSelf)
	a := tr.Observe(parseTestNode(t, fbCallNode("started", "voice", testPeer, "0", testCallID, testT0)))
	tr.Observe(parseTestNode(t, fbCallNode("ended", "voice", testPeer, "0", testCallID, testT0.Add(10*time.Second))))
	// Call back within the ended window: different call id, so a new call.
	b := tr.Observe(parseTestNode(t, fbCallNode("started", "voice", testSelf, "0", testCallID2, testT0.Add(15*time.Second))))
	if b == nil || b.Kind != noticeOutgoing || b.MessageID() == a.MessageID() {
		t.Fatalf("second call: %+v", b)
	}
}

func TestCallTrackerLSOnly(t *testing.T) {
	tr := newCallTracker(testSelf)
	tbl := &table.LSTable{
		LSUpdateThreadOngoingCallState: []*table.LSUpdateThreadOngoingCallState{{ThreadKey: testPeer, OngoingCallState: table.RtcCallStateVideo1to1}},
		LSUpdateOrInsertRtcOngoingCallData: []*table.LSUpdateOrInsertRtcOngoingCallData{{
			ThreadKey:    testPeer,
			Unrecognized: map[int]any{1: int64(1758235000000), 2: "not a handle", 3: testCallID},
		}},
	}
	events, keys := lsCallEvents(tbl, testT0)
	if len(events) != 1 || keys[0] != testPeer || events[0].CallID != testCallID || !events[0].Video {
		t.Fatalf("ls events: %+v %v", events, keys)
	}
	var notices []*callNotice
	for _, e := range events {
		e.Portal = testPortal
		if n := tr.Observe(e); n != nil {
			notices = append(notices, n)
		}
	}
	if len(notices) != 1 || notices[0].Kind != noticeOngoing || notices[0].Sender != 0 ||
		notices[0].Text() != "📞 Video call started — join on Messenger" {
		t.Fatalf("notices: %+v", notices)
	}
	end := &table.LSTable{
		LSUpdateThreadOngoingCallState: []*table.LSUpdateThreadOngoingCallState{{ThreadKey: testPeer}},
		LSDeleteRtcOngoingCallData:     []*table.LSDeleteRtcOngoingCallData{{ThreadKey: testPeer}},
	}
	events, _ = lsCallEvents(end, testT0.Add(time.Minute))
	notices = nil
	for _, e := range events {
		e.Portal = testPortal
		if n := tr.Observe(e); n != nil {
			notices = append(notices, n)
		}
	}
	if len(notices) != 1 || notices[0].Text() != "Call ended" {
		t.Fatalf("end notices: %+v", notices)
	}
	// A row whose index-1 guess is not a call state is ignored.
	bad := &table.LSTable{LSUpdateThreadOngoingCallState: []*table.LSUpdateThreadOngoingCallState{{ThreadKey: testPeer, OngoingCallState: 1758235000000}}}
	if events, _ = lsCallEvents(bad, testT0); len(events) != 0 {
		t.Fatalf("invalid state not ignored: %+v", events)
	}
}
