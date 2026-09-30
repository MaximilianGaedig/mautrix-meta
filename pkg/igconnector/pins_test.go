package igconnector

import (
	"testing"

	"maunium.net/go/mautrix/bridgev2"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

func TestPinChanges(t *testing.T) {
	got := pinChanges(&slidetypes.PinMessageEvent{
		PinnedMessages:   []string{"mid.new"},
		UnpinnedMessages: []string{"mid.old"},
	})
	want := []bridgev2.PinChange{
		{MessageID: metaid.MakeFBMessageID("mid.old"), Pinned: false},
		{MessageID: metaid.MakeFBMessageID("mid.new"), Pinned: true},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("change %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
	if len(pinChanges(&slidetypes.PinMessageEvent{})) != 0 {
		t.Error("an empty pin event changes nothing")
	}
}

func TestPinRequest(t *testing.T) {
	req, err := pinRequest(metaid.MakeFBPortalID(123456), metaid.MakeFBMessageID("mid.x"))
	if err != nil {
		t.Fatal(err)
	}
	if req.ThreadID != "123456" || req.MessageID != "mid.x" {
		t.Errorf("request = %+v", req)
	}
}
