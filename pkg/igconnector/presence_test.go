package igconnector

import (
	"context"
	"maps"
	"sync"
	"testing"
	"time"

	"go.mau.fi/util/jsontime"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/presence"
)

// sentPresence collects what a presence manager sends to Matrix.
type sentPresence struct {
	lock sync.Mutex
	sent map[string]event.Presence
}

func (sp *sentPresence) send(_ context.Context, remoteUserID string, p event.Presence) error {
	sp.lock.Lock()
	defer sp.lock.Unlock()
	if sp.sent == nil {
		sp.sent = make(map[string]event.Presence)
	}
	sp.sent[remoteUserID] = p
	return nil
}

// flush runs the manager once and returns everything sent so far.
func (sp *sentPresence) flush(m *presence.Manager) map[string]event.Presence {
	m.Tick(context.Background())
	sp.lock.Lock()
	defer sp.lock.Unlock()
	return maps.Clone(sp.sent)
}

// testIGClient is a client of the made-up login 1000 with nothing to connect to. With bridging,
// its connector has a presence manager whose output lands in the returned sentPresence.
func testIGClient(bridging bool) (*IGClient, *sentPresence) {
	sp := &sentPresence{}
	main := &IGConnector{Config: Config{PresenceBridging: bridging}}
	if bridging {
		main.presence = presence.NewManager(presence.Config{}, sp.send)
	}
	ic := &IGClient{
		Main:      main,
		UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "1000"}},
	}
	ic.mailboxProcessed.Store(true)
	return ic, sp
}

func message(sender int64, at time.Time) *slidetypes.Message {
	return &slidetypes.Message{SenderFBID: sender, TimestampMS: jsontime.UnixMilliString{Time: at}}
}

func receipt(reader int64) *slidetypes.ReadReceiptEvent {
	return &slidetypes.ReadReceiptEvent{ReadReceipt: slidetypes.ReadReceipt{
		ParticipantFBID: reader,
		// The time of the message that was read, long ago: it must not be taken for when it was read.
		WatermarkTimestampMS: jsontime.UnixMilliString{Time: time.Now().Add(-24 * time.Hour)},
	}}
}

func TestDeltaIsLive(t *testing.T) {
	for _, tc := range []struct {
		name                string
		catchingUpTo, seqID int64
		live                bool
	}{
		{"connected with nothing missed", 0, 50, true},
		{"replayed backlog", 100, 60, false},
		{"last of the backlog", 100, 100, false},
		{"first new delta after the backlog", 100, 101, true},
	} {
		if got := deltaIsLive(tc.catchingUpTo, tc.seqID); got != tc.live {
			t.Errorf("%s: deltaIsLive(%d, %d) = %v, want %v", tc.name, tc.catchingUpTo, tc.seqID, got, tc.live)
		}
	}
}

func TestDeltaActivity(t *testing.T) {
	now := time.UnixMilli(1790000000000)
	sentAt := now.Add(-3 * time.Second)

	fbid, at, ok := deltaActivity(&slidetypes.Delta{Data: &slidetypes.NewMessageEvent{Message: message(42, sentAt)}}, false, now)
	if !ok || fbid != 42 || !at.Equal(sentAt) {
		t.Errorf("a message counts with its own time even in a replay: got %d %v %v", fbid, at, ok)
	}
	fbid, at, ok = deltaActivity(&slidetypes.Delta{Data: &slidetypes.AdminMessageEvent{Message: message(43, sentAt)}}, true, now)
	if !ok || fbid != 43 || !at.Equal(sentAt) {
		t.Errorf("admin message: got %d %v %v", fbid, at, ok)
	}
	fbid, at, ok = deltaActivity(&slidetypes.Delta{Data: receipt(44)}, true, now)
	if !ok || fbid != 44 || !at.Equal(now) {
		t.Errorf("a live read receipt counts as of now: got %d %v %v", fbid, at, ok)
	}
	if _, _, ok = deltaActivity(&slidetypes.Delta{Data: receipt(44)}, false, now); ok {
		t.Error("a replayed read receipt says nothing about now")
	}
	for name, data := range map[string]slidetypes.DeltaEvent{
		"message without a body":   &slidetypes.NewMessageEvent{},
		"message without a sender": &slidetypes.NewMessageEvent{Message: message(0, sentAt)},
		"receipt without a reader": receipt(0),
		"reaction":                 &slidetypes.CreateReactionEvent{},
		"deleted message":          &slidetypes.DeleteMessageEvent{},
		"our own mark-read":        &slidetypes.MarkReadEvent{},
	} {
		if _, _, ok = deltaActivity(&slidetypes.Delta{Data: data}, true, now); ok {
			t.Errorf("%s must not count as activity", name)
		}
	}
}

func TestNoteActivity(t *testing.T) {
	ic, sp := testIGClient(true)
	ic.noteActivity(42, time.Now())
	ic.noteActivity(1000, time.Now())                // ourselves
	ic.noteActivity(0, time.Now())                   // nobody
	ic.noteActivity(43, time.Now().Add(-time.Hour))  // backfilled: over long ago
	ic.noteActivity(44, time.Time{})                 // no time given: now
	ic.noteActivity(45, time.Now().Add(time.Minute)) // a clock ahead of ours: now
	got := sp.flush(ic.Main.presence)
	want := map[string]event.Presence{"42": event.PresenceOnline, "44": event.PresenceOnline, "45": event.PresenceOnline}
	if !maps.Equal(got, want) {
		t.Errorf("sent %v, want %v", got, want)
	}
}

// With presence_bridging off there is no manager, and nothing may be noted or crash.
func TestNoteActivityDisabled(t *testing.T) {
	ic, sp := testIGClient(false)
	ic.noteActivity(42, time.Now())
	ic.noteDeltaActivity(&slidetypes.Delta{Data: receipt(42)})
	if len(sp.sent) != 0 {
		t.Errorf("sent %v with presence bridging off", sp.sent)
	}
}

// The deltas Instagram sends reach the presence manager through the event handler itself. None
// of these name a thread, so the handler stops at the portal lookup, after the activity is noted.
func TestDeltaMarksSenderOnline(t *testing.T) {
	ic, sp := testIGClient(true)
	ctx := context.Background()
	deltas := []*slidetypes.Delta{
		{TypeName: "SlideUQPPReadReceipt", UQSeqID: 7, Data: receipt(42)},
		{TypeName: "SlideUQPPNewMessage", UQSeqID: 8, Data: &slidetypes.NewMessageEvent{Message: message(43, time.Now())}},
		{TypeName: "SlideUQPPNewMessage", UQSeqID: 9, Data: &slidetypes.NewMessageEvent{Message: message(1000, time.Now())}},
		{TypeName: "SlideUQPPNewMessage", UQSeqID: 10, Data: &slidetypes.NewMessageEvent{Message: message(44, time.Now().Add(-time.Hour))}},
	}
	for _, d := range deltas {
		if err := ic.handleIGEvent(ctx, d); err != nil {
			t.Fatalf("%s: %v", d.TypeName, err)
		}
	}
	got := sp.flush(ic.Main.presence)
	want := map[string]event.Presence{"42": event.PresenceOnline, "43": event.PresenceOnline}
	if !maps.Equal(got, want) {
		t.Errorf("sent %v, want %v", got, want)
	}
}

// After a reconnect the missed deltas are replayed. A read receipt among them happened at some
// unknown earlier time, so it must not make the reader online now; the first new one does.
func TestReplayedReceiptIsNotActivity(t *testing.T) {
	ic, sp := testIGClient(true)
	ctx := context.Background()
	ic.catchingUpTo = 100
	if err := ic.handleIGEvent(ctx, &slidetypes.Delta{TypeName: "SlideUQPPReadReceipt", UQSeqID: 90, Data: receipt(42)}); err != nil {
		t.Fatal(err)
	}
	if got := sp.flush(ic.Main.presence); len(got) != 0 {
		t.Fatalf("replayed receipt sent %v", got)
	}
	if err := ic.handleIGEvent(ctx, &slidetypes.Delta{TypeName: "SlideUQPPReadReceipt", UQSeqID: 101, Data: receipt(42)}); err != nil {
		t.Fatal(err)
	}
	if got := sp.flush(ic.Main.presence); got["42"] != event.PresenceOnline {
		t.Errorf("live receipt sent %v", got)
	}
}
