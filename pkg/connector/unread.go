package connector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"maunium.net/go/mautrix/bridgev2"

	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

var _ bridgev2.MarkedUnreadHandlingNetworkAPI = (*MetaClient)(nil)

var errUnreadEncryptedUnsupported = errors.New("an encrypted Messenger chat can't be marked unread from Matrix: its devices tell each other with a message the bridge can't send yet")

// markedUnreadRequest is what a Matrix unread mark means on Messenger: either a task for the socket or a
// request of the older chat code, and neither when there is nothing to send.
//
// Taking the mark off is the task that marks the thread read, up to now. Putting it on has no task that was
// found in the web client, so it uses the endpoint the older chat code has for it. An encrypted chat keeps
// its read state in the messages its devices send each other (a chatRead metadata sync action), which the
// bridge doesn't send: read receipts already mark such a chat read, and marking it unread isn't possible.
func markedUnreadRequest(portal *bridgev2.Portal, unread bool, now time.Time) (socket.Task, *httpclient.MercuryThreadStatus, error) {
	encrypted := portal.Metadata.(*metaid.PortalMetadata).ThreadType.IsWhatsApp()
	threadID := metaid.ParseFBPortalID(portal.ID)
	switch {
	case encrypted && unread:
		return nil, nil, errUnreadEncryptedUnsupported
	case encrypted:
		return nil, nil, nil
	case unread:
		return nil, httpclient.NewMercuryReadStatus(threadID, false), nil
	default:
		return &socket.ThreadMarkReadTask{
			ThreadId:            threadID,
			LastReadWatermarkTs: now.UnixMilli(),
			SyncGroup:           1,
		}, nil, nil
	}
}

// HandleMarkedUnread marks the chat unread (or read again) on Messenger when it's marked in Matrix.
func (m *MetaClient) HandleMarkedUnread(ctx context.Context, msg *bridgev2.MatrixMarkedUnread) error {
	if m.LoginMeta.Cookies == nil {
		return bridgev2.ErrNotLoggedIn
	} else if !m.LoginMeta.Platform.IsMessenger() {
		return fmt.Errorf("marking chats unread is only bridged to Messenger")
	}
	task, status, err := markedUnreadRequest(msg.Portal, msg.Content.Unread, time.Now())
	if err != nil {
		return err
	} else if task != nil {
		_, err = m.Client.ExecuteTasks(ctx, task)
		return err
	} else if status != nil {
		return m.Client.GetHTTP().SendMercuryThreadStatus(ctx, status)
	}
	return nil
}
