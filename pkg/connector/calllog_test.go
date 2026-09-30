package connector

import (
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/calllog"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
)

type callLine struct {
	Type bridgev2.RemoteEventType
	Text string
	ID   networkid.MessageID
}

// feedCalls runs a call notification through the tracker and the call log, like queueCallNotice does, and
// says what would be shown.
func feedCalls(t *testing.T, tr *callTracker, log *calllog.Log, self int64, evt *callEvent) []callLine {
	t.Helper()
	n := tr.Observe(evt)
	if n == nil {
		return nil
	}
	sender := bridgev2.EventSender{Sender: networkid.UserID(fmtInt(n.Sender)), IsFromMe: n.Sender == self}
	var lines []callLine
	for _, report := range callLogReports(log, n, sender) {
		msg, ok := report.(*simplevent.Message[*calllog.Call])
		if !ok {
			t.Fatalf("report is %T", report)
		}
		lines = append(lines, callLine{report.GetType(), msg.Data.Text(), msg.ID})
	}
	return lines
}

func TestCallLogIncomingMissedIsOneLineThatUpdates(t *testing.T) {
	tr, log := newCallTracker(testSelf), calllog.New()
	start := feedCalls(t, tr, log, testSelf, parseTestNode(t, fbCallNode("started", "voice", testPeer, "0", testCallID, testT0)))
	if len(start) != 1 || start[0].Type != bridgev2.RemoteEventMessage || start[0].Text != "Incoming voice call" {
		t.Fatalf("start = %+v", start)
	}
	end := feedCalls(t, tr, log, testSelf, parseTestNode(t, fbCallNode("missed", "voice", testPeer, "0", testCallID, testT0.Add(20*time.Second))))
	if len(end) != 1 || end[0].Type != bridgev2.RemoteEventEdit || end[0].Text != "Missed voice call" {
		t.Fatalf("end = %+v", end)
	}
	if start[0].ID != end[0].ID {
		t.Errorf("the end must edit the line of the start: %q vs %q", start[0].ID, end[0].ID)
	}
}

func TestCallLogAnsweredCallShowsItsDuration(t *testing.T) {
	tr, log := newCallTracker(testSelf), calllog.New()
	feedCalls(t, tr, log, testSelf, parseTestNode(t, fbCallNode("started", "video", testPeer, "0", testCallID, testT0)))
	end := feedCalls(t, tr, log, testSelf, parseTestNode(t, fbCallNode("ended", "video", testPeer, "74", testCallID, testT0.Add(80*time.Second))))
	if len(end) != 1 || end[0].Type != bridgev2.RemoteEventEdit || end[0].Text != "Video call, 1:14" {
		t.Fatalf("end = %+v", end)
	}
}

func TestCallLogOutgoingCalls(t *testing.T) {
	tr, log := newCallTracker(testSelf), calllog.New()
	start := feedCalls(t, tr, log, testSelf, parseTestNode(t, fbCallNode("started", "voice", testSelf, "0", testCallID, testT0)))
	if len(start) != 1 || start[0].Text != "Outgoing voice call" {
		t.Fatalf("start = %+v", start)
	}
	end := feedCalls(t, tr, log, testSelf, parseTestNode(t, fbCallNode("missed", "voice", testSelf, "0", testCallID, testT0.Add(30*time.Second))))
	if len(end) != 1 || end[0].Text != "Cancelled voice call" {
		t.Fatalf("end = %+v", end)
	}
	feedCalls(t, tr, log, testSelf, parseTestNode(t, fbCallNode("started", "voice", testSelf, "0", testCallID2, testT0.Add(time.Hour))))
	answered := feedCalls(t, tr, log, testSelf, parseTestNode(t, fbCallNode("ended", "voice", testSelf, "5", testCallID2, testT0.Add(time.Hour+10*time.Second))))
	if len(answered) != 1 || answered[0].Text != "Voice call, 0:05" {
		t.Fatalf("answered = %+v", answered)
	}
}

func TestCallLogEndWithoutASeenStartStillShowsTheOutcome(t *testing.T) {
	tr, log := newCallTracker(testSelf), calllog.New()
	// Without the start the tracker doesn't know who called, so it can only say that the call ended.
	lines := feedCalls(t, tr, log, testSelf, parseTestNode(t, fbCallNode("missed", "voice", testPeer, "0", testCallID, testT0)))
	if len(lines) != 2 || lines[0].Type != bridgev2.RemoteEventMessage || lines[1].Type != bridgev2.RemoteEventEdit || lines[1].Text != "Voice call ended" {
		t.Fatalf("lines = %+v", lines)
	}
	if lines[0].ID != lines[1].ID {
		t.Errorf("IDs differ: %q %q", lines[0].ID, lines[1].ID)
	}
}

func TestCallLogRepeatedReportsDoNotPostTwice(t *testing.T) {
	tr, log := newCallTracker(testSelf), calllog.New()
	feedCalls(t, tr, log, testSelf, parseTestNode(t, fbCallNode("started", "voice", testPeer, "0", testCallID, testT0)))
	// The same end seen on the other channel too (Lightspeed after the E2EE notification).
	feedCalls(t, tr, log, testSelf, parseTestNode(t, fbCallNode("ended", "voice", testPeer, "12", testCallID, testT0.Add(15*time.Second))))
	again := feedCalls(t, tr, log, testSelf, &callEvent{Source: callSourceLS, Kind: callEnded, Portal: testPortal, Time: testT0.Add(19 * time.Second)})
	if len(again) != 0 {
		t.Errorf("repeated end = %+v", again)
	}
}
