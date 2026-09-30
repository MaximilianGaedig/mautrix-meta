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

func TestForwardedMessengerMessageIsLabelled(t *testing.T) {
	mc := &MessageConverter{}
	msg := &table.WrappedMessage{LSInsertMessage: &table.LSInsertMessage{Text: "look at this", IsForwarded: true, ForwardScore: 1}}
	cm := mc.ToMatrix(context.Background(), nil, nil, nil, nil, "mid.x", msg)
	if got := cm.Parts[0].Content.Body; got != "↷ Forwarded\n\nlook at this" {
		t.Errorf("body = %q", got)
	}
	msg.IsForwarded = false
	cm = mc.ToMatrix(context.Background(), nil, nil, nil, nil, "mid.x", msg)
	if got := cm.Parts[0].Content.Body; got != "look at this" {
		t.Errorf("an ordinary message must not be labelled: %q", got)
	}
}

func TestCallLogAdminText(t *testing.T) {
	for _, text := range []string{
		"You missed a video call from Anna.",
		"You missed an audio call from Anna.",
		"You missed a group call",
		"You missed a call from a contact.",
		"The video call ended.",
		"The call ended.",
	} {
		if !isCallLogAdminText(text) {
			t.Errorf("%q is a call log text", text)
		}
	}
	for _, text := range []string{
		"Anna named the group Trip.",
		"You missed the bus",
		"Anna changed the theme.",
		"",
	} {
		if isCallLogAdminText(text) {
			t.Errorf("%q is not a call log text", text)
		}
	}
}

func TestAdminTextAboutACallIsNotBridgedWhenTheCallLogShowsIt(t *testing.T) {
	msg := &table.WrappedMessage{LSInsertMessage: &table.LSInsertMessage{Text: "You missed a video call from Anna.", IsAdminMessage: true}}
	cm := (&MessageConverter{CallLogShown: true}).ToMatrix(context.Background(), nil, nil, nil, nil, "mid.x", msg)
	if len(cm.Parts) != 1 || !cm.Parts[0].DontBridge {
		t.Fatalf("the call log line says it already: %+v", cm.Parts)
	}
	cm = (&MessageConverter{}).ToMatrix(context.Background(), nil, nil, nil, nil, "mid.x", msg)
	if cm.Parts[0].DontBridge {
		t.Error("without a call log the admin text is all there is to say about the call")
	}
	other := &table.WrappedMessage{LSInsertMessage: &table.LSInsertMessage{Text: "Anna named the group Trip.", IsAdminMessage: true}}
	if cm := (&MessageConverter{CallLogShown: true}).ToMatrix(context.Background(), nil, nil, nil, nil, "mid.x", other); cm.Parts[0].DontBridge {
		t.Error("other admin texts stay")
	}
	chat := &table.WrappedMessage{LSInsertMessage: &table.LSInsertMessage{Text: "You missed a call from Anna, call me"}}
	if cm := (&MessageConverter{CallLogShown: true}).ToMatrix(context.Background(), nil, nil, nil, nil, "mid.x", chat); cm.Parts[0].DontBridge {
		t.Error("a person's message that reads like a call log is still a message")
	}
}
