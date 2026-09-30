package igconnector

import (
	"context"
	"fmt"
	"strconv"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

var _ bridgev2.PinHandlingNetworkAPI = (*IGClient)(nil)

// pinChanges turns a pin event into the pins it adds and removes.
func pinChanges(evt *slidetypes.PinMessageEvent) []bridgev2.PinChange {
	changes := make([]bridgev2.PinChange, 0, len(evt.PinnedMessages)+len(evt.UnpinnedMessages))
	for _, id := range evt.UnpinnedMessages {
		changes = append(changes, bridgev2.PinChange{MessageID: metaid.MakeFBMessageID(id), Pinned: false})
	}
	for _, id := range evt.PinnedMessages {
		changes = append(changes, bridgev2.PinChange{MessageID: metaid.MakeFBMessageID(id), Pinned: true})
	}
	return changes
}

func (ic *IGClient) handlePinMessages(key networkid.PortalKey, evt *slidetypes.PinMessageEvent) bridgev2.EventHandlingResult {
	changes := pinChanges(evt)
	if len(changes) == 0 {
		return bridgev2.EventHandlingResultIgnored
	}
	return ic.UserLogin.QueueRemoteEvent(&simplevent.ChatInfoChange{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventChatInfoChange,
			PortalKey: key,
		},
		ChatInfoChange: &bridgev2.ChatInfoChange{
			ChatInfo: &bridgev2.ChatInfo{PinChanges: changes},
		},
	})
}

// pinRequest builds the request that pins or unpins a message in a thread.
func pinRequest(portalID networkid.PortalID, target networkid.MessageID) (*slidetypes.PinMessageRequest, error) {
	msgID, ok := metaid.ParseMessageID(target).(metaid.ParsedFBMessageID)
	if !ok {
		return nil, fmt.Errorf("unexpected parsed message ID type")
	}
	return &slidetypes.PinMessageRequest{
		ThreadID:  strconv.FormatInt(metaid.ParseFBPortalID(portalID), 10),
		MessageID: msgID.ID,
	}, nil
}

func (ic *IGClient) HandleMatrixPin(ctx context.Context, msg *bridgev2.MatrixPin) error {
	if ic.LoginMeta.Cookies == nil {
		return bridgev2.ErrNotLoggedIn
	}
	req, err := pinRequest(msg.Portal.ID, msg.TargetMessage.ID)
	if err != nil {
		return err
	}
	if msg.Pinned {
		_, err = ic.Client.PinMessage(ctx, req)
	} else {
		_, err = ic.Client.UnpinMessage(ctx, req)
	}
	return err
}
