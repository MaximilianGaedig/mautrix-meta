package connector

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

var _ bridgev2.TagHandlingNetworkAPI = (*MetaClient)(nil)

var errUnarchiveEncryptedUnsupported = errors.New("an encrypted Messenger chat can't be taken out of the archive from Matrix: a message or the Messenger app does")

// archiveUserLocal is the tag a chat gets from the folder Messenger keeps it in: the archive tag for archived
// chats, and nothing for the others, so that tags of other kinds are left alone. Without an archive tag
// configured, archiving isn't bridged.
func archiveUserLocal(archiveTag event.RoomTag, folder string) *bridgev2.UserLocalPortalInfo {
	if archiveTag == "" || folder != folderArchived {
		return nil
	}
	return &bridgev2.UserLocalPortalInfo{Tag: &archiveTag}
}

// applyArchive puts the archive tag on the info of a chat that is in the archived folder.
func (m *MetaClient) applyArchive(info *bridgev2.ChatInfo, folder string) {
	local := archiveUserLocal(m.Main.Config.ArchiveTag, folder)
	if local == nil {
		return
	}
	if info.UserLocal == nil {
		info.UserLocal = local
	} else {
		info.UserLocal.Tag = local.Tag
	}
}

// userLocalForFolderMove is the tag that follows a chat being moved into the archive (the archive tag), or out of
// it to the inbox (no tag).
func userLocalForFolderMove(archiveTag event.RoomTag, archived bool) *bridgev2.UserLocalPortalInfo {
	if archiveTag == "" {
		return nil
	}
	tag := event.RoomTag("")
	if archived {
		tag = archiveTag
	}
	return &bridgev2.UserLocalPortalInfo{Tag: &tag}
}

func (m *MetaClient) handleFolderMove(tk handlerParams, archived bool, changeType string) bridgev2.RemoteEvent {
	local := userLocalForFolderMove(m.Main.Config.ArchiveTag, archived)
	if local == nil {
		return nil
	}
	if tk.Sync != nil {
		if tk.Sync.Info.UserLocal == nil {
			tk.Sync.Info.UserLocal = local
		} else {
			tk.Sync.Info.UserLocal.Tag = local.Tag
		}
		return nil
	}
	return m.wrapChatInfoChange(tk.ID, 0, tk.Type, &bridgev2.ChatInfoChange{
		ChatInfo: &bridgev2.ChatInfo{UserLocal: local},
	}, changeType)
}

func (m *MetaClient) handleMoveThreadToArchived(tk handlerParams, _ *table.LSMoveThreadToArchivedFolder) bridgev2.RemoteEvent {
	return m.handleFolderMove(tk, true, "LSMoveThreadToArchivedFolder")
}

func (m *MetaClient) handleMoveThreadToInbox(tk handlerParams, _ *table.LSMoveThreadToInboxAndUpdateParent) bridgev2.RemoteEvent {
	return m.handleFolderMove(tk, false, "LSMoveThreadToInboxAndUpdateParent")
}

// unarchiveRequest takes a chat out of the archive. The socket has no task for that in the web client code
// that was captured (the remove types of DeleteThreadTask only go into the archive), Messenger does it by
// itself when a message comes. The older chat code has an endpoint that sets the archived status either way,
// MercuryServerRequests.changeThreadArchivedStatus, which is what this uses.
//
// An encrypted chat is archived through the Messenger thread it replaced, and nothing says that taking that
// thread out of the archive brings the encrypted one back, so it is refused.
func unarchiveRequest(portal *bridgev2.Portal) (*httpclient.MercuryThreadStatus, error) {
	if portal.Metadata.(*metaid.PortalMetadata).ThreadType.IsWhatsApp() {
		return nil, errUnarchiveEncryptedUnsupported
	}
	return httpclient.NewMercuryArchivedStatus(metaid.ParseFBPortalID(portal.ID), false), nil
}

// HandleRoomTag archives a Messenger chat when the archive tag is added to its room, and takes it out of the
// archive when the tag is removed. A tag update that doesn't change the archive tag is ignored, so that tags
// of other kinds never move a chat.
func (m *MetaClient) HandleRoomTag(ctx context.Context, msg *bridgev2.MatrixRoomTag) error {
	archive := bridgev2.TagChange(msg, m.Main.Config.ArchiveTag)
	if archive == nil {
		return nil
	} else if m.LoginMeta.Cookies == nil {
		return bridgev2.ErrNotLoggedIn
	} else if *archive {
		_, err := m.Client.ExecuteTasks(ctx, m.threadRemoveTask(msg.Portal, socket.RemoveTypeArchive))
		return err
	} else if !m.LoginMeta.Platform.IsMessenger() {
		return fmt.Errorf("taking chats out of the archive is only bridged to Messenger")
	}
	status, err := unarchiveRequest(msg.Portal)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Can't unarchive Messenger chat")
		return err
	}
	return m.Client.GetHTTP().SendMercuryThreadStatus(ctx, status)
}

// threadRemoveTask deletes or archives a chat: they are the same task on Messenger.
func (m *MetaClient) threadRemoveTask(portal *bridgev2.Portal, removeType int64) *socket.DeleteThreadTask {
	portalMeta := portal.Metadata.(*metaid.PortalMetadata)
	threadID := metaid.ParseFBPortalID(portal.ID)
	syncGroup := int64(1)
	if portalMeta.ThreadType.IsWhatsApp() && portalMeta.FBThreadKey != 0 {
		threadID = portalMeta.FBThreadKey
		syncGroup = 95
	}
	return &socket.DeleteThreadTask{
		ThreadKey:  threadID,
		RemoveType: removeType,
		SyncGroup:  syncGroup,
	}
}
