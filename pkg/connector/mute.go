package connector

import (
	"context"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

var _ bridgev2.MuteHandlingNetworkAPI = (*MetaClient)(nil)

// muteExpireTimeMS is a Matrix mute as Messenger stores it, the reverse of handleUpdateMuteSetting: -1 for
// muted until turned back on, 0 for not muted, otherwise when it ends.
func muteExpireTimeMS(content *event.BeeperMuteEventContent) int64 {
	until := content.GetMutedUntilTime()
	switch {
	case until == event.MutedForever:
		return -1
	case until.IsZero():
		return 0
	}
	return until.UnixMilli()
}

// HandleMute mutes or unmutes the chat on Messenger when it's muted in Matrix.
func (m *MetaClient) HandleMute(ctx context.Context, msg *bridgev2.MatrixMute) error {
	if m.LoginMeta.Cookies == nil {
		return bridgev2.ErrNotLoggedIn
	}
	_, err := m.Client.ExecuteTasks(ctx, &socket.MuteThreadTask{
		ThreadKey:        metaid.ParseFBPortalID(msg.Portal.ID),
		MuteExpireTimeMS: muteExpireTimeMS(msg.Content),
		SyncGroup:        1,
	})
	return err
}
