package igconnector

import (
	"context"
	"fmt"
	"strconv"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

var _ bridgev2.PowerLevelHandlingNetworkAPI = (*IGClient)(nil)

// adminChange is +1 when a power level change makes someone a group admin (powerAdmin and up, as the chat
// info maps admins), -1 when it stops them being one, and 0 otherwise.
func adminChange(change *bridgev2.SinglePowerLevelChange) int {
	wasAdmin, isAdmin := change.OrigLevel >= powerAdmin, change.NewLevel >= powerAdmin
	switch {
	case isAdmin && !wasAdmin:
		return 1
	case wasAdmin && !isAdmin:
		return -1
	}
	return 0
}

// HandleMatrixPowerLevels makes people admins of an Instagram group, or not, from the room's power levels.
func (ic *IGClient) HandleMatrixPowerLevels(ctx context.Context, msg *bridgev2.MatrixPowerLevelChange) (bool, error) {
	if msg.Portal.RoomType == database.RoomTypeDM {
		return false, nil
	}
	if ic.LoginMeta.Cookies == nil {
		return false, bridgev2.ErrNotLoggedIn
	}
	var add, remove []string
	for _, change := range msg.Users {
		direction := adminChange(&change.SinglePowerLevelChange)
		if direction == 0 {
			continue
		}
		var fbid int64
		switch target := change.Target.(type) {
		case *bridgev2.Ghost:
			fbid = metaid.ParseUserID(target.ID)
		case *bridgev2.UserLogin:
			fbid = metaid.ParseUserLoginID(target.ID)
		default:
			continue
		}
		// Instagram names people by their Instagram ID here, not the messaging ID the ghosts use.
		igid, err := ic.Main.DB.GetIGUserForFBID(ctx, fbid)
		if err != nil {
			return false, err
		} else if igid == "" {
			return false, fmt.Errorf("don't know the Instagram ID of %d", fbid)
		}
		if direction > 0 {
			add = append(add, igid)
		} else {
			remove = append(remove, igid)
		}
	}
	threadID := strconv.FormatInt(metaid.ParseFBPortalID(msg.Portal.ID), 10)
	changed := false
	if len(add) > 0 {
		if _, err := ic.Client.AddAdmins(ctx, &slidetypes.ModifyAdminsRequest{ThreadID: threadID, UserIGIDs: add}); err != nil {
			return false, fmt.Errorf("failed to add admins: %w", err)
		}
		changed = true
	}
	if len(remove) > 0 {
		if _, err := ic.Client.RemoveAdmins(ctx, &slidetypes.ModifyAdminsRequest{ThreadID: threadID, UserIGIDs: remove}); err != nil {
			return changed, fmt.Errorf("failed to remove admins: %w", err)
		}
		changed = true
	}
	return changed, nil
}
