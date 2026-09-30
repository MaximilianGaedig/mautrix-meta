package connector

import (
	"context"
	"strings"
	"testing"
	"time"

	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"maunium.net/go/mautrix/event"
)

var (
	testGroupJID = waTypes.NewJID("120363000000000001", waTypes.GroupServer)
	testUserJID  = waTypes.NewJID("100000000000001", waTypes.MessengerServer)
)

func TestGroupInfoNameTopicChange(t *testing.T) {
	m := testMetaClient()
	sender := testUserJID
	ts := time.UnixMilli(1758290000000)
	change := m.waChatInfoEvent(&events.GroupInfo{
		JID:       testGroupJID,
		Sender:    &sender,
		Timestamp: ts,
		Name:      &waTypes.GroupName{Name: "Weekend plans"},
		Topic:     &waTypes.GroupTopic{Topic: "Where and when"},
	})
	if change == nil || change.ChatInfoChange.ChatInfo == nil {
		t.Fatalf("a name and topic change must be bridged: %+v", change)
	}
	info := change.ChatInfoChange.ChatInfo
	if info.Name == nil || *info.Name != "Weekend plans" || info.Topic == nil || *info.Topic != "Where and when" {
		t.Errorf("info = %+v", info)
	}
	if change.ChatInfoChange.MemberChanges != nil {
		t.Errorf("no member change expected: %+v", change.ChatInfoChange.MemberChanges)
	}
	if !change.Timestamp.Equal(ts) || change.Sender.Sender != "100000000000001" || !strings.Contains(string(change.PortalKey.ID), "120363000000000001") {
		t.Errorf("meta = %+v", change.EventMeta)
	}
}

func TestGroupInfoTopicDeletedAndDisappearTimer(t *testing.T) {
	m := testMetaClient()
	change := m.waChatInfoEvent(&events.GroupInfo{
		JID:       testGroupJID,
		Topic:     &waTypes.GroupTopic{Topic: "stale", TopicDeleted: true},
		Ephemeral: &waTypes.GroupEphemeral{IsEphemeral: true, DisappearingTimer: 86400},
	})
	if change == nil {
		t.Fatal("no event")
	}
	info := change.ChatInfoChange.ChatInfo
	if info.Topic == nil || *info.Topic != "" {
		t.Errorf("a deleted topic must clear the topic: %+v", info.Topic)
	}
	if info.Disappear == nil || info.Disappear.Timer != 24*time.Hour || info.Disappear.Type != event.DisappearingTypeAfterSend {
		t.Errorf("disappear = %+v", info.Disappear)
	}
	off := m.waChatInfoEvent(&events.GroupInfo{JID: testGroupJID, Ephemeral: &waTypes.GroupEphemeral{}})
	if off == nil {
		t.Fatal("no event for the timer turning off")
	}
	if d := off.ChatInfoChange.ChatInfo.Disappear; d == nil || d.Timer != 0 || d.Type != event.DisappearingTypeNone {
		t.Errorf("turning the timer off: %+v", d)
	}
}

func TestGroupInfoMembersStillBridged(t *testing.T) {
	m := testMetaClient()
	change := m.waChatInfoEvent(&events.GroupInfo{JID: testGroupJID, Join: []waTypes.JID{testUserJID}})
	if change == nil || change.ChatInfoChange.MemberChanges == nil || len(change.ChatInfoChange.MemberChanges.MemberMap) != 1 || change.ChatInfoChange.ChatInfo != nil {
		t.Errorf("change = %+v", change)
	}
	if m.waChatInfoEvent(&events.GroupInfo{JID: testGroupJID, Locked: &waTypes.GroupLocked{IsLocked: true}}) != nil {
		t.Error("a change the bridge does not show must not produce an event")
	}
}

func TestGroupPictureChange(t *testing.T) {
	m := testMetaClient()
	change := m.waChatInfoEvent(&events.Picture{JID: testGroupJID, Author: testUserJID, PictureID: "1758290000"})
	if change == nil {
		t.Fatal("a group picture change must be bridged")
	}
	avatar := change.ChatInfoChange.ChatInfo.Avatar
	if avatar == nil || avatar.ID != "1758290000" || avatar.Remove || avatar.Get == nil {
		t.Errorf("avatar = %+v", avatar)
	}
	removed := m.waChatInfoEvent(&events.Picture{JID: testGroupJID, Remove: true})
	if removed == nil {
		t.Fatal("no event for the removal")
	}
	if a := removed.ChatInfoChange.ChatInfo.Avatar; a == nil || !a.Remove {
		t.Errorf("removal = %+v", a)
	}
	if _, err := avatar.Get(context.Background()); err == nil {
		t.Error("with no connection the avatar cannot be fetched")
	}
	if m.waChatInfoEvent(&events.Picture{JID: testUserJID, PictureID: "1"}) != nil {
		t.Error("a user's profile picture is not a chat picture")
	}
}

func TestUndecryptableMessageNotice(t *testing.T) {
	m := testMetaClient()
	evt := &events.UndecryptableMessage{}
	evt.Info.Chat = testGroupJID
	evt.Info.Sender = testUserJID
	evt.Info.ID = "3EB0FAKEID"
	evt.Info.Timestamp = time.UnixMilli(1758290000000)
	notice := m.waUndecryptableMessage(evt)
	if notice == nil {
		t.Fatal("an undecryptable message must produce a notice")
	}
	if !strings.HasPrefix(string(notice.ID), "undecryptable:") || !strings.HasSuffix(string(notice.ID), ":3EB0FAKEID") {
		t.Errorf("the notice needs an ID of its own so the real message is not dropped as a duplicate: %q", notice.ID)
	}
	cm, err := notice.ConvertMessageFunc(context.Background(), nil, nil, evt)
	if err != nil || len(cm.Parts) != 1 {
		t.Fatalf("convert: %v %+v", err, cm)
	}
	c := cm.Parts[0].Content
	if c.MsgType != event.MsgNotice || !strings.Contains(c.Body, "couldn't be decrypted") {
		t.Errorf("content = %+v", c)
	}
	evt.UnavailableType = events.UnavailableTypeViewOnce
	cm, _ = notice.ConvertMessageFunc(context.Background(), nil, nil, evt)
	if !strings.Contains(cm.Parts[0].Content.Body, "view once") || !strings.Contains(cm.Parts[0].Content.Body, "Messenger app") {
		t.Errorf("view once = %q", cm.Parts[0].Content.Body)
	}
}

func TestUndecryptableHiddenFailuresAreSkipped(t *testing.T) {
	m := testMetaClient()
	evt := &events.UndecryptableMessage{DecryptFailMode: events.DecryptFailHide}
	evt.Info.Chat = testGroupJID
	if m.waUndecryptableMessage(evt) != nil {
		t.Error("a failure the protocol marks hidden must not be shown")
	}
	status := &events.UndecryptableMessage{}
	status.Info.Chat = waTypes.StatusBroadcastJID
	if m.waUndecryptableMessage(status) != nil {
		t.Error("status broadcasts are not bridged")
	}
}
