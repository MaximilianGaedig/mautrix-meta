package connector

import (
	"encoding/json"
	"errors"
	"testing"

	"go.mau.fi/util/configupgrade"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

func tagUpdate(before []event.RoomTag, after ...event.RoomTag) *bridgev2.MatrixRoomTag {
	build := func(tags []event.RoomTag) *event.TagEventContent {
		content := &event.TagEventContent{Tags: event.Tags{}}
		for _, tag := range tags {
			content.Tags[tag] = event.TagMetadata{}
		}
		return content
	}
	msg := &bridgev2.MatrixRoomTag{PrevContent: build(before)}
	msg.Content = build(after)
	return msg
}

func TestArchiveUserLocal(t *testing.T) {
	if got := archiveUserLocal("m.lowpriority", folderArchived); got == nil || *got.Tag != "m.lowpriority" {
		t.Errorf("archived chat: %+v", got)
	}
	if got := archiveUserLocal("m.lowpriority", folderInbox); got != nil {
		t.Errorf("an inbox chat must keep its tags: %+v", got)
	}
	if got := archiveUserLocal("", folderArchived); got != nil {
		t.Errorf("without an archive tag archiving is not bridged: %+v", got)
	}
}

func TestChatInfoOfArchivedChatGetsTheTag(t *testing.T) {
	m := testMetaClient()
	m.Main.Config.ArchiveTag = "m.lowpriority"
	thread := &table.LSUpdateOrInsertThread{ThreadKey: 4242, ThreadType: table.ONE_TO_ONE, FolderName: folderArchived}
	info := m.wrapChatInfo(thread)
	if info.UserLocal == nil || info.UserLocal.Tag == nil || *info.UserLocal.Tag != "m.lowpriority" {
		t.Fatalf("user local = %+v", info.UserLocal)
	}
	thread.FolderName = folderInbox
	if info := m.wrapChatInfo(thread); info.UserLocal != nil && info.UserLocal.Tag != nil {
		t.Errorf("inbox chat got a tag: %v", *info.UserLocal.Tag)
	}
	m.Main.Config.ArchiveTag = ""
	thread.FolderName = folderArchived
	if info := m.wrapChatInfo(thread); info.UserLocal != nil && info.UserLocal.Tag != nil {
		t.Errorf("tag is off but the chat got %v", *info.UserLocal.Tag)
	}
}

func TestMovingAThreadToTheArchiveTagsItLive(t *testing.T) {
	m := testMetaClient()
	m.Main.Config.ArchiveTag = "m.lowpriority"
	tk := handlerParams{ID: 4242, Type: table.ONE_TO_ONE, Portal: m.makeFBPortalKey(4242, table.ONE_TO_ONE)}
	change, ok := m.handleMoveThreadToArchived(tk, &table.LSMoveThreadToArchivedFolder{ThreadKey: 4242}).(*simplevent.ChatInfoChange)
	if !ok {
		t.Fatal("moving to the archive must be bridged when there is an archive tag")
	}
	if local := change.ChatInfoChange.ChatInfo.UserLocal; local == nil || *local.Tag != "m.lowpriority" {
		t.Errorf("user local = %+v", local)
	}
	change, ok = m.handleMoveThreadToInbox(tk, &table.LSMoveThreadToInboxAndUpdateParent{ThreadKey: 4242}).(*simplevent.ChatInfoChange)
	if !ok {
		t.Fatal("moving back to the inbox must be bridged when there is an archive tag")
	}
	if local := change.ChatInfoChange.ChatInfo.UserLocal; local == nil || *local.Tag != "" {
		t.Errorf("moving back to the inbox must clear the tag: %+v", local)
	}
	m.Main.Config.ArchiveTag = ""
	if m.handleMoveThreadToArchived(tk, &table.LSMoveThreadToArchivedFolder{ThreadKey: 4242}) != nil {
		t.Error("nothing to bridge without an archive tag")
	}
}

func TestMovingAThreadDuringASyncTagsTheSync(t *testing.T) {
	m := testMetaClient()
	m.Main.Config.ArchiveTag = "m.lowpriority"
	sync := &FBChatResync{Info: &bridgev2.ChatInfo{}}
	tk := handlerParams{ID: 4242, Type: table.ONE_TO_ONE, Sync: sync}
	if evt := m.handleMoveThreadToArchived(tk, &table.LSMoveThreadToArchivedFolder{ThreadKey: 4242}); evt != nil {
		t.Errorf("a thread being synced folds the change into the sync: %v", evt)
	}
	if sync.Info.UserLocal == nil || *sync.Info.UserLocal.Tag != "m.lowpriority" {
		t.Fatalf("sync info = %+v", sync.Info.UserLocal)
	}
	if evt := m.handleMoveThreadToInbox(tk, &table.LSMoveThreadToInboxAndUpdateParent{ThreadKey: 4242}); evt != nil {
		t.Errorf("event = %v", evt)
	}
	if *sync.Info.UserLocal.Tag != "" {
		t.Errorf("moving back to the inbox must clear the tag, got %q", *sync.Info.UserLocal.Tag)
	}
}

func TestArchiveFromTag(t *testing.T) {
	const tag = event.RoomTag("m.lowpriority")
	if archive, err := archiveFromTag(tag, tagUpdate(nil, tag)); !archive || err != nil {
		t.Errorf("tag added: %v %v", archive, err)
	}
	if archive, err := archiveFromTag(tag, tagUpdate([]event.RoomTag{tag}, tag, "m.favourite")); archive || err != nil {
		t.Errorf("an unrelated tag added while archived must not archive again: %v %v", archive, err)
	}
	if archive, err := archiveFromTag(tag, tagUpdate(nil, "m.favourite")); archive || err != nil {
		t.Errorf("an unrelated tag: %v %v", archive, err)
	}
	if archive, err := archiveFromTag(tag, tagUpdate([]event.RoomTag{tag})); archive || !errors.Is(err, errUnarchiveUnsupported) {
		t.Errorf("tag removed: %v %v", archive, err)
	}
	if archive, err := archiveFromTag("", tagUpdate(nil, tag)); archive || err != nil {
		t.Errorf("no archive tag configured: %v %v", archive, err)
	}
}

func TestArchiveTaskIsADeleteThreadTaskWithRemoveTypeOne(t *testing.T) {
	m := testMetaClient()
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: metaid.MakeFBPortalID(4242)},
		Metadata:  &metaid.PortalMetadata{},
	}}
	task := m.threadRemoveTask(portal, socket.RemoveTypeArchive)
	payload, queue := task.Create()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if task.GetLabel() != "146" || queue != "4242" || string(data) != `{"thread_key":4242,"remove_type":1,"sync_group":1}` {
		t.Errorf("label %q queue %q payload %s", task.GetLabel(), queue, data)
	}
	hybrid := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: "100000000000002@msgr"},
		Metadata:  &metaid.PortalMetadata{ThreadType: table.ENCRYPTED_OVER_WA_ONE_TO_ONE, FBThreadKey: 777},
	}}
	if task := m.threadRemoveTask(hybrid, socket.RemoveTypeDelete); task.ThreadKey != 777 || task.SyncGroup != 95 || task.RemoveType != 0 {
		t.Errorf("hybrid delete task = %+v", task)
	}
}

func TestArchiveTagConfig(t *testing.T) {
	var cfg Config
	if err := yaml.Unmarshal([]byte(ExampleConfig), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.ArchiveTag != "" {
		t.Errorf("archiving must be off by default, got %q", cfg.ArchiveTag)
	}
	var base, old yaml.Node
	if err := yaml.Unmarshal([]byte(ExampleConfig), &base); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal([]byte("displayname_template: '{{.DisplayName}}'\narchive_tag: m.lowpriority\n"), &old); err != nil {
		t.Fatal(err)
	}
	upgradeConfig(configupgrade.NewHelper(base.Content[0], old.Content[0]))
	upgraded, err := yaml.Marshal(&base)
	if err != nil {
		t.Fatal(err)
	}
	var kept Config
	if err := yaml.Unmarshal(upgraded, &kept); err != nil {
		t.Fatal(err)
	}
	if kept.ArchiveTag != "m.lowpriority" {
		t.Errorf("upgrading a config must keep the archive tag, got %q", kept.ArchiveTag)
	}
}
