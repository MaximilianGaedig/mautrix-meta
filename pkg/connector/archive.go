package connector

import (
	"context"
	"errors"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

var _ bridgev2.TagHandlingNetworkAPI = (*MetaClient)(nil)

var errUnarchiveUnsupported = errors.New("Messenger has no way to take a chat out of the archive from Matrix, a message or the Messenger app does")

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

// archiveFromTag says what a Matrix tag update means for archiving: archive the chat when the archive tag was
// added. Taking a chat out of the archive has no task in the web client, Messenger does it by itself when
// a message comes, so that is an error. An update that doesn't concern the archive tag is neither.
func archiveFromTag(archiveTag event.RoomTag, msg *bridgev2.MatrixRoomTag) (archive bool, err error) {
	change := bridgev2.TagChange(msg, archiveTag)
	if change == nil {
		return false, nil
	} else if !*change {
		return false, errUnarchiveUnsupported
	}
	return true, nil
}

// HandleRoomTag archives a Messenger chat when the archive tag is added to its room.
func (m *MetaClient) HandleRoomTag(ctx context.Context, msg *bridgev2.MatrixRoomTag) error {
	archive, err := archiveFromTag(m.Main.Config.ArchiveTag, msg)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Can't unarchive Messenger chat")
		return err
	} else if !archive {
		return nil
	}
	if m.LoginMeta.Cookies == nil {
		return bridgev2.ErrNotLoggedIn
	}
	_, err = m.Client.ExecuteTasks(ctx, m.threadRemoveTask(msg.Portal, socket.RemoveTypeArchive))
	return err
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
