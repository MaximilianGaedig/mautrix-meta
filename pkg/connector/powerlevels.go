package connector

import (
	"context"
	"fmt"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

var _ bridgev2.PowerLevelHandlingNetworkAPI = (*MetaClient)(nil)

// adminTaskFor is the change to someone's group admin status that a power level change stands for, or nil.
// Messenger groups have one kind of admin, so any level from moderator up is admin; Messenger then reports
// them back at the admin level (fbPowerAdmin).
func adminTaskFor(threadKey, contactID int64, change *bridgev2.SinglePowerLevelChange) *socket.UpdateAdminTask {
	wasAdmin, isAdmin := change.OrigLevel >= fbPowerModerator, change.NewLevel >= fbPowerModerator
	if wasAdmin == isAdmin || change.OrigLevel >= fbPowerSuperAdmin {
		return nil
	}
	task := &socket.UpdateAdminTask{ThreadKey: threadKey, ContactID: contactID}
	if isAdmin {
		task.IsAdmin = 1
	}
	return task
}

// HandleMatrixPowerLevels makes people group admins, or not, from the room's power levels.
func (m *MetaClient) HandleMatrixPowerLevels(ctx context.Context, msg *bridgev2.MatrixPowerLevelChange) (bool, error) {
	if msg.Portal.RoomType == database.RoomTypeDM {
		return false, nil
	}
	if m.LoginMeta.Cookies == nil {
		return false, bridgev2.ErrNotLoggedIn
	}
	threadKey := metaid.ParseFBPortalID(msg.Portal.ID)
	var tasks []socket.Task
	for _, change := range msg.Users {
		var contactID int64
		switch target := change.Target.(type) {
		case *bridgev2.Ghost:
			contactID = metaid.ParseUserID(target.ID)
		case *bridgev2.UserLogin:
			contactID = metaid.ParseUserLoginID(target.ID)
		default:
			continue
		}
		if task := adminTaskFor(threadKey, contactID, &change.SinglePowerLevelChange); task != nil {
			tasks = append(tasks, task)
		}
	}
	if len(tasks) == 0 {
		return false, nil
	}
	if _, err := m.Client.ExecuteTasks(ctx, tasks...); err != nil {
		return false, fmt.Errorf("failed to change group admins: %w", err)
	}
	return true, nil
}
