package connector

import (
	"context"
	"fmt"
	"time"

	"go.mau.fi/util/ptr"
	"go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/metaid"
	"go.mau.fi/mautrix-meta/pkg/msgconv/mediadl"
)

// waGroupInfoChange is what an encrypted group's info event changed: who joined or left, and the name,
// topic and disappearing timer. It is nil when the event changed nothing the bridge shows.
func (m *MetaClient) waGroupInfoChange(evt *events.GroupInfo) *simplevent.ChatInfoChange {
	change := &bridgev2.ChatInfoChange{}
	memberChanges := &bridgev2.ChatMemberList{
		MemberMap: make(map[networkid.UserID]bridgev2.ChatMember),
	}
	for _, userID := range evt.Join {
		memberChanges.MemberMap.Set(bridgev2.ChatMember{
			EventSender: m.makeWAEventSender(userID),
			Membership:  event.MembershipJoin,
		})
	}
	for _, userID := range evt.Leave {
		memberChanges.MemberMap.Set(bridgev2.ChatMember{
			EventSender:    m.makeWAEventSender(userID),
			Membership:     event.MembershipLeave,
			PrevMembership: event.MembershipJoin,
		})
	}
	if len(memberChanges.MemberMap) > 0 {
		change.MemberChanges = memberChanges
	}
	var info bridgev2.ChatInfo
	if evt.Name != nil {
		info.Name = ptr.Ptr(evt.Name.Name)
	}
	if evt.Topic != nil {
		topic := evt.Topic.Topic
		if evt.Topic.TopicDeleted {
			topic = ""
		}
		info.Topic = &topic
	}
	if evt.Ephemeral != nil {
		disappear := &database.DisappearingSetting{}
		if evt.Ephemeral.IsEphemeral && evt.Ephemeral.DisappearingTimer > 0 {
			disappear.Type = event.DisappearingTypeAfterSend
			disappear.Timer = time.Duration(evt.Ephemeral.DisappearingTimer) * time.Second
		}
		info.Disappear = disappear
	}
	if info.Name != nil || info.Topic != nil || info.Disappear != nil {
		change.ChatInfo = &info
	}
	if change.ChatInfo == nil && change.MemberChanges == nil {
		return nil
	}
	eventMeta := simplevent.EventMeta{
		Type:      bridgev2.RemoteEventChatInfoChange,
		PortalKey: m.makeWAPortalKey(evt.JID),
		Timestamp: evt.Timestamp,
	}
	if evt.Sender != nil {
		eventMeta.Sender = m.makeWAEventSender(*evt.Sender)
	}
	return &simplevent.ChatInfoChange{EventMeta: eventMeta, ChatInfoChange: change}
}

// waChatInfoEvent is the chat info change for a group info or picture event, or nil when there is none.
func (m *MetaClient) waChatInfoEvent(rawEvt any) *simplevent.ChatInfoChange {
	switch evt := rawEvt.(type) {
	case *events.GroupInfo:
		return m.waGroupInfoChange(evt)
	case *events.Picture:
		return m.waGroupPictureChange(evt)
	}
	return nil
}

// waGroupAvatar is the avatar an encrypted group changed to, or the removal of it.
func (m *MetaClient) waGroupAvatar(jid waTypes.JID, pictureID string, remove bool) *bridgev2.Avatar {
	if remove || pictureID == "" {
		return &bridgev2.Avatar{Remove: true}
	}
	return &bridgev2.Avatar{
		ID: networkid.AvatarID(pictureID),
		Get: func(ctx context.Context) ([]byte, error) {
			cli := m.E2EEClient
			if cli == nil {
				return nil, ErrNotConnected
			}
			pic, err := cli.GetProfilePictureInfo(ctx, jid, &whatsmeow.GetProfilePictureParams{ExistingID: pictureID})
			if err != nil {
				return nil, fmt.Errorf("failed to get group picture: %w", err)
			} else if pic == nil {
				return nil, fmt.Errorf("group picture %s is no longer available", pictureID)
			}
			return mediadl.DownloadAvatar(ctx, pic.URL)
		},
	}
}

// waGroupPictureChange is a change of the picture of an encrypted group. Other chats' pictures are the
// members' avatars, which are synced with the profiles.
func (m *MetaClient) waGroupPictureChange(evt *events.Picture) *simplevent.ChatInfoChange {
	if evt.JID.Server != waTypes.GroupServer {
		return nil
	}
	eventMeta := simplevent.EventMeta{
		Type:      bridgev2.RemoteEventChatInfoChange,
		PortalKey: m.makeWAPortalKey(evt.JID),
		Timestamp: evt.Timestamp,
	}
	if !evt.Author.IsEmpty() {
		eventMeta.Sender = m.makeWAEventSender(evt.Author)
	}
	return &simplevent.ChatInfoChange{
		EventMeta: eventMeta,
		ChatInfoChange: &bridgev2.ChatInfoChange{ChatInfo: &bridgev2.ChatInfo{
			Avatar: m.waGroupAvatar(evt.JID, evt.PictureID, evt.Remove),
		}},
	}
}

func undecryptableNotice(appName string, unavailable events.UnavailableType, fromMe bool) string {
	if unavailable == events.UnavailableTypeViewOnce {
		if fromMe {
			return "You sent a view once message from another device."
		}
		return "You received a view once message. For added privacy, you can only open it in the " + appName + "."
	}
	return "A message couldn't be decrypted. The sender's device may still send it again."
}

// waUndecryptableMessage (nil when the failure is meant to be hidden) is the notice for an encrypted message that arrived but couldn't be decrypted. It has
// an ID of its own: the message itself may still arrive later, and it must not be dropped as a duplicate.
func (m *MetaClient) waUndecryptableMessage(evt *events.UndecryptableMessage) *simplevent.Message[*events.UndecryptableMessage] {
	if evt.DecryptFailMode == events.DecryptFailHide || evt.Info.Chat == waTypes.StatusBroadcastJID {
		return nil
	}
	appName := "Messenger app"
	if m.LoginMeta != nil && m.LoginMeta.Platform.IsInstagram() {
		appName = "Instagram app"
	}
	return &simplevent.Message[*events.UndecryptableMessage]{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventMessage,
			PortalKey:    m.makeWAPortalKey(evt.Info.Chat),
			Sender:       m.makeWAEventSender(evt.Info.Sender),
			CreatePortal: true,
			Timestamp:    evt.Info.Timestamp,
		},
		ID:   networkid.MessageID("undecryptable:" + string(metaid.MakeWAMessageID(evt.Info.Chat, evt.Info.Sender, evt.Info.ID))),
		Data: evt,
		ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data *events.UndecryptableMessage) (*bridgev2.ConvertedMessage, error) {
			return &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
				Type: event.EventMessage,
				Content: &event.MessageEventContent{
					MsgType: event.MsgNotice,
					Body:    undecryptableNotice(appName, data.UnavailableType, data.Info.IsFromMe),
				},
				Extra: map[string]any{"fi.mau.meta.undecryptable": true},
			}}}, nil
		},
	}
}
