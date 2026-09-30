package msgconv

import (
	"context"
	"testing"

	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

func adminMessage(shownByState bool) *table.WrappedMessage {
	return &table.WrappedMessage{
		LSInsertMessage:       &table.LSInsertMessage{Text: "Someone changed the theme.", IsAdminMessage: true},
		AdminTextShownByState: shownByState,
	}
}

func TestAdminMessagesAreBridged(t *testing.T) {
	mc := &MessageConverter{}
	cm := mc.ToMatrix(context.Background(), nil, nil, nil, nil, "mid.x", adminMessage(false))
	if len(cm.Parts) != 1 || cm.Parts[0].DontBridge {
		t.Fatalf("a service message must be bridged: %+v", cm.Parts)
	}
	if cm.Parts[0].Content.MsgType != event.MsgNotice || cm.Parts[0].Content.Body != "Someone changed the theme." {
		t.Errorf("content = %+v", cm.Parts[0].Content)
	}
}

func TestAdminMessagesShownByStateAreNot(t *testing.T) {
	mc := &MessageConverter{}
	cm := mc.ToMatrix(context.Background(), nil, nil, nil, nil, "mid.x", adminMessage(true))
	if len(cm.Parts) != 1 || !cm.Parts[0].DontBridge {
		t.Fatalf("an admin text that room state already shows must not be bridged: %+v", cm.Parts)
	}
}
