package connector

import (
	"slices"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

func TestDeliveryWatermarks(t *testing.T) {
	var marks deliveryWatermarks
	chat := networkid.PortalKey{ID: "1", Receiver: "me"}
	other := networkid.PortalKey{ID: "2", Receiver: "me"}
	noon := time.UnixMilli(1_760_000_000_000)

	expect := func(what string, portal networkid.PortalKey, upTo time.Time, wantNews bool, wantAfter time.Time) {
		t.Helper()
		after, news := marks.advance(portal, upTo)
		if news != wantNews {
			t.Fatalf("%s: news = %v, want %v", what, news, wantNews)
		}
		if news && !after.Equal(wantAfter) {
			t.Fatalf("%s: covers what came after %v, want %v", what, after, wantAfter)
		}
	}

	expect("nothing known about the chat yet", chat, noon, true, noon.Add(-deliveryReceiptLookback))
	expect("the same time again", chat, noon, false, time.Time{})
	expect("an earlier time", chat, noon.Add(-time.Minute), false, time.Time{})
	expect("a later time", chat, noon.Add(time.Minute), true, noon)
	// The earlier time above did not move the chat back
	expect("a later time still", chat, noon.Add(2*time.Minute), true, noon.Add(time.Minute))
	expect("another chat, on its own", other, noon, true, noon.Add(-deliveryReceiptLookback))
}

func TestDeliveryTargets(t *testing.T) {
	if got := deliveryTargets(nil); len(got) != 0 {
		t.Fatalf("no messages: got %v", got)
	}

	// A message bridged as several parts is one message
	parts := []*database.Message{
		{ID: "mid.1", PartID: ""},
		{ID: "mid.2", PartID: "0"},
		{ID: "mid.2", PartID: "1"},
		{ID: "mid.3", PartID: ""},
	}
	want := []networkid.MessageID{"mid.1", "mid.2", "mid.3"}
	if got := deliveryTargets(parts); !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
