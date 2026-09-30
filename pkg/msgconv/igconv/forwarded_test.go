package igconv

import (
	"context"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
)

func TestForwardedInstagramMessageIsLabelled(t *testing.T) {
	mc := &MessageConverter{}
	msg := &slidetypes.Message{
		IGDIsForwarded: true,
		Content:        slidetypes.MessageContentWrapper{Content: &slidetypes.MessageContentText{TextBody: "look at this"}},
	}
	cm := mc.ToMatrix(context.Background(), nil, nil, nil, nil, "mid.x", msg, true)
	if got := cm.Parts[0].Content.Body; got != "↷ Forwarded\n\nlook at this" {
		t.Errorf("body = %q", got)
	}
	msg.IGDIsForwarded = false
	cm = mc.ToMatrix(context.Background(), nil, nil, nil, nil, "mid.x", msg, true)
	if got := cm.Parts[0].Content.Body; got != "look at this" {
		t.Errorf("an ordinary message must not be labelled: %q", got)
	}
}
