package connector

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

var _ bridgev2.PinHandlingNetworkAPI = (*MetaClient)(nil)

// threadPins is what one table says about the pinned messages of one thread.
type threadPins struct {
	ThreadKey int64
	Info      *bridgev2.ChatInfo
}

func (tp *threadPins) GetThreadKey() int64 {
	return tp.ThreadKey
}

// pinsByThread reads the pin rows of a table. A thread sync clears a thread's pins and then sets
// each pinned message again, so a cleared thread gets its whole pin list; otherwise each row is
// one message pinned or unpinned.
func pinsByThread(tbl *table.LSTable) []*threadPins {
	var out []*threadPins
	byThread := make(map[int64]*threadPins)
	get := func(threadKey int64) *threadPins {
		tp, ok := byThread[threadKey]
		if !ok {
			tp = &threadPins{ThreadKey: threadKey, Info: &bridgev2.ChatInfo{}}
			byThread[threadKey] = tp
			out = append(out, tp)
		}
		return tp
	}
	for _, clear := range tbl.LSClearPinnedMessages {
		get(clear.ThreadKey).Info.PinnedMessages = &[]networkid.MessageID{}
	}
	sets := slices.Clone(tbl.LSSetPinnedMessage)
	slices.SortStableFunc(sets, func(a, b *table.LSSetPinnedMessage) int {
		return cmp.Compare(a.PinnedTimestampMs, b.PinnedTimestampMs)
	})
	for _, set := range sets {
		tp := get(set.ThreadKey)
		msgID := metaid.MakeFBMessageID(set.MessageId)
		pinned := set.PinnedMessageState == 1
		if tp.Info.PinnedMessages != nil {
			list := slices.DeleteFunc(*tp.Info.PinnedMessages, func(id networkid.MessageID) bool { return id == msgID })
			if pinned {
				list = append(list, msgID)
			}
			tp.Info.PinnedMessages = &list
		} else {
			tp.Info.PinChanges = append(tp.Info.PinChanges, bridgev2.PinChange{MessageID: msgID, Pinned: pinned})
		}
	}
	return out
}

func (m *MetaClient) handlePins(tk handlerParams, tp *threadPins) bridgev2.RemoteEvent {
	// Pins in encrypted chats have not been seen yet; their message IDs may not be the plain FB IDs
	// used here. Log what arrives so the first real one shows its shape.
	zerolog.Ctx(tk.ctx).Info().
		Int64("thread_key", tp.ThreadKey).
		Int64("thread_type", int64(tk.Type)).
		Any("pin_changes", tp.Info.PinChanges).
		Any("pinned_messages", tp.Info.PinnedMessages).
		Msg("Received pinned messages")
	return m.wrapChatInfoChange(tk.ID, 0, tk.Type, &bridgev2.ChatInfoChange{ChatInfo: tp.Info}, "pins")
}

func (m *MetaClient) HandleMatrixPin(ctx context.Context, msg *bridgev2.MatrixPin) error {
	if m.LoginMeta.Cookies == nil {
		return bridgev2.ErrNotLoggedIn
	}
	messageID, ok := metaid.ParseMessageID(msg.TargetMessage.ID).(metaid.ParsedFBMessageID)
	if !ok {
		return fmt.Errorf("pinning is not supported in encrypted chats")
	}
	state := 0
	if msg.Pinned {
		state = 1
	}
	_, err := m.Client.ExecuteTasks(ctx, &socket.SetPinnedMessageTask{
		ThreadKey:          metaid.ParseFBPortalID(msg.Portal.ID),
		MessageID:          messageID.ID,
		PinnedMessageState: state,
	})
	return err
}
