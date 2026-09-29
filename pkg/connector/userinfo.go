package connector

import (
	"context"
	"fmt"
	"net/url"
	"path"

	"github.com/rs/zerolog"
	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	"go.mau.fi/mautrix-meta/pkg/metaid"
	"go.mau.fi/mautrix-meta/pkg/msgconv/mediadl"
)

func (m *MetaClient) GetUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	if ghost.Name == "" {
		contactID := metaid.ParseUserID(ghost.ID)
		resp, err := m.Client.ExecuteTasks(ctx, &socket.GetContactsFullTask{
			ContactID: contactID,
		})
		if err != nil {
			return nil, err
		}
		for _, info := range resp.LSDeleteThenInsertIGContactInfo {
			err := m.Main.DB.PutFBIDForIGUser(ctx, info.IgId, info.ContactId)
			if err != nil {
				zerolog.Ctx(ctx).Warn().Err(err).Msg("Failed to save FBID for IG user")
			}
		}
		if len(resp.LSDeleteThenInsertContact) > 0 {
			return m.wrapUserInfo(resp.LSDeleteThenInsertContact[0]), nil
		} else {
			return nil, fmt.Errorf("user info not found via GetContactsFullTask")
		}
	}
	return nil, nil
}

const (
	MetaAIInstagramID = 656175869434325
	MetaAIMessengerID = 156025504001094
)

// RelationshipProfileKey carries how you know somebody on the network into the ghost's Matrix
// profile, so a client can say "friend" or "not a friend" beside their name without asking the
// bridge anything. Its own key rather than a display-name suffix: a client that does not know about
// it ignores it, and one that does can render it however it likes.
const RelationshipProfileKey = "im.mxg.relationship"

// relationshipHaver is whatever a contact row that states the viewer's relationship looks like.
// Two of messagix's tables carry it and neither is part of types.UserInfo, which is the smallest
// thing every caller of wrapUserInfo has in common.
type relationshipHaver interface {
	GetContactViewerRelationship() table.ContactViewerRelationship
}

func (m *MetaClient) wrapUserInfo(info types.UserInfo) *bridgev2.UserInfo {
	wrapped := &bridgev2.UserInfo{
		Name: ptr.Ptr(m.Main.Config.FormatDisplayname(DisplaynameParams{
			DisplayName: info.GetName(),
			Username:    info.GetUsername(),
			ID:          info.GetFBID(),
		})),
		Avatar: wrapAvatar(info.GetAvatarURL()),
		IsBot:  ptr.Ptr(info.GetFBID() == MetaAIInstagramID || info.GetFBID() == MetaAIMessengerID), // TODO do this in a less hardcoded way?
	}
	// Only when the network actually said: an unknown relationship is the absence of an answer, and
	// writing "none" for it would claim we asked and were told no.
	if rel, ok := info.(relationshipHaver); ok {
		if name := rel.GetContactViewerRelationship().Name(); name != "" {
			wrapped.ExtraProfile = database.ExtraProfile{}
			if err := wrapped.ExtraProfile.Set(RelationshipProfileKey, name); err != nil {
				wrapped.ExtraProfile = nil
			}
		}
	}
	return wrapped
}

func wrapAvatar(avatarURL string) *bridgev2.Avatar {
	if avatarURL == "" {
		return &bridgev2.Avatar{Remove: true}
	}
	parsedURL, _ := url.Parse(avatarURL)
	avatarID := path.Base(parsedURL.Path)
	return &bridgev2.Avatar{
		ID: networkid.AvatarID(avatarID),
		Get: func(ctx context.Context) ([]byte, error) {
			return mediadl.DownloadAvatar(ctx, avatarURL)
		},
	}
}
