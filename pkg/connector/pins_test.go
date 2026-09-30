package connector

import (
	"encoding/json"
	"reflect"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

func fbMsg(id string) networkid.MessageID {
	return metaid.MakeFBMessageID(id)
}

func TestPinsByThread(t *testing.T) {
	got := pinsByThread(&table.LSTable{
		// Thread 1 is synced: its pins are cleared, then set again, in no particular order.
		LSClearPinnedMessages: []*table.LSClearPinnedMessages{{ThreadKey: 1}, {ThreadKey: 3}},
		LSSetPinnedMessage: []*table.LSSetPinnedMessage{
			{ThreadKey: 1, MessageId: "mid.later", PinnedTimestampMs: 200, PinnedMessageState: 1},
			{ThreadKey: 2, MessageId: "mid.pinned", PinnedTimestampMs: 150, PinnedMessageState: 1},
			{ThreadKey: 1, MessageId: "mid.earlier", PinnedTimestampMs: 100, PinnedMessageState: 1},
			{ThreadKey: 2, MessageId: "mid.unpinned", PinnedTimestampMs: 160, PinnedMessageState: 0},
		},
	})
	want := map[int64]*bridgev2.ChatInfo{
		1: {PinnedMessages: &[]networkid.MessageID{fbMsg("mid.earlier"), fbMsg("mid.later")}},
		2: {PinChanges: []bridgev2.PinChange{
			{MessageID: fbMsg("mid.pinned"), Pinned: true},
			{MessageID: fbMsg("mid.unpinned"), Pinned: false},
		}},
		// Cleared and nothing set again: nothing is pinned any more.
		3: {PinnedMessages: &[]networkid.MessageID{}},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d threads, want %d", len(got), len(want))
	}
	for _, tp := range got {
		if !reflect.DeepEqual(tp.Info, want[tp.ThreadKey]) {
			t.Errorf("thread %d: got %+v, want %+v", tp.ThreadKey, tp.Info, want[tp.ThreadKey])
		}
	}
	if pinsByThread(&table.LSTable{}) != nil {
		t.Error("a table without pin rows must not touch any thread")
	}
}

func TestSetPinnedMessageTask(t *testing.T) {
	task := &socket.SetPinnedMessageTask{ThreadKey: 42, MessageID: "mid.x", PinnedMessageState: 1}
	payload, queue := task.Create()
	if task.GetLabel() != "751" || queue != "set_pinned_message_search" {
		t.Fatalf("label %q, queue %q", task.GetLabel(), queue)
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	// The same fields the web client puts in this task.
	if string(data) != `{"thread_key":42,"message_id":"mid.x","pinned_message_state":1}` {
		t.Errorf("payload = %s", data)
	}
	unpin, _ := json.Marshal(&socket.SetPinnedMessageTask{ThreadKey: 42, MessageID: "mid.x"})
	if string(unpin) != `{"thread_key":42,"message_id":"mid.x","pinned_message_state":0}` {
		t.Errorf("unpin payload = %s", unpin)
	}
}

func TestThreadsWithStateChanges(t *testing.T) {
	identity := func(k int64) int64 { return k }
	got := threadsWithStateChanges(&table.LSTable{
		LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{{ThreadKey: 1}},
		LSRemoveParticipantFromThread:   []*table.LSRemoveParticipantFromThread{{ThreadKey: 2}},
		LSSyncUpdateThreadName:          []*table.LSSyncUpdateThreadName{{ThreadKey: 3}},
		LSSetThreadImageURL:             []*table.LSSetThreadImageURL{{ThreadKey: 4}},
		LSSetPinnedMessage:              []*table.LSSetPinnedMessage{{ThreadKey: 5}},
	}, identity)
	for k := int64(1); k <= 5; k++ {
		if !got.Has(k) {
			t.Errorf("thread %d changed state", k)
		}
	}
	// A theme or nickname change has no room state: its admin text is all there is.
	if got.Has(6) || len(threadsWithStateChanges(&table.LSTable{LSUpdateThreadTheme: []*table.LSUpdateThreadTheme{{}}}, identity)) != 0 {
		t.Error("no state change, no thread")
	}
	// Encrypted threads are keyed by their WhatsApp thread key.
	mapped := threadsWithStateChanges(&table.LSTable{
		LSSyncUpdateThreadName: []*table.LSSyncUpdateThreadName{{ThreadKey: 10}},
	}, func(k int64) int64 { return k + 100 })
	if !mapped.Has(110) {
		t.Errorf("thread key not mapped: %v", mapped)
	}
}
