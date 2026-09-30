package connector

import (
	"context"
	"testing"

	"go.mau.fi/util/ptr"
	"go.mau.fi/whatsmeow/proto/instamadilloDeleteMessage"
	"go.mau.fi/whatsmeow/proto/instamadilloSupplementMessage"
	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-meta/pkg/metaid"
	"go.mau.fi/mautrix-meta/pkg/msgconv"
)

func igEventClient(existing ...networkid.MessageID) *MetaClient {
	m := testMetaClient()
	m.Main.Bridge.BackgroundCtx = context.Background()
	m.Main.MsgConv = &msgconv.MessageConverter{
		RecentMessages: func(context.Context, networkid.PortalKey, int) ([]*database.Message, error) {
			var out []*database.Message
			for _, id := range existing {
				out = append(out, &database.Message{ID: id})
			}
			return out, nil
		},
	}
	return m
}

func TestInstagramEncryptedReactionEditDeleteTargets(t *testing.T) {
	chat := waTypes.NewJID("100000000000002", waTypes.MessengerServer)
	author := waTypes.NewJID("100000000000003", waTypes.MessengerServer)
	target := metaid.MakeWAMessageID(chat, author, "TARGETOTID1")
	m := igEventClient("fb:mid.unrelated", target)
	fbMsg := func(msg any) *events.FBMessage {
		evt := &events.FBMessage{}
		evt.Info.Chat = chat
		evt.Info.Sender = author
		switch typed := msg.(type) {
		case *instamadilloSupplementMessage.SupplementMessagePayload:
			evt.Message = typed
		case *instamadilloDeleteMessage.DeleteMessagePayload:
			evt.Message = typed
		}
		return evt
	}
	reaction := &instamadilloSupplementMessage.SupplementMessagePayload{
		TargetMessageOtid: ptr.Ptr("TARGETOTID1"),
		Content: &instamadilloSupplementMessage.SupplementMessageContent{SupplementMessageContent: &instamadilloSupplementMessage.SupplementMessageContent_Reaction{
			Reaction: &instamadilloSupplementMessage.Reaction{Emoji: ptr.Ptr("😂")},
		}},
	}
	edit := &instamadilloSupplementMessage.SupplementMessagePayload{
		TargetMessageOtid: ptr.Ptr("TARGETOTID1"),
		Content: &instamadilloSupplementMessage.SupplementMessageContent{SupplementMessageContent: &instamadilloSupplementMessage.SupplementMessageContent_EditText{
			EditText: &instamadilloSupplementMessage.EditText{NewContent: ptr.Ptr("fixed"), EditCount: ptr.Ptr(int32(1))},
		}},
	}
	del := &instamadilloDeleteMessage.DeleteMessagePayload{MessageOtid: ptr.Ptr("TARGETOTID1")}
	for name, msg := range map[string]any{"reaction": reaction, "edit": edit, "delete": del} {
		evt := &WAMessageEvent{FBMessage: fbMsg(msg), m: m}
		if got := evt.GetTargetMessage(); got != target {
			t.Errorf("%s: target = %q, want %q", name, got, target)
		}
	}
	unknown := &instamadilloDeleteMessage.DeleteMessagePayload{MessageOtid: ptr.Ptr("NOTHERE")}
	if got := (&WAMessageEvent{FBMessage: fbMsg(unknown), m: m}).GetTargetMessage(); got != "" {
		t.Errorf("unknown message: target = %q", got)
	}
}
