package connector

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

func unreadTestPortal(id networkid.PortalID, threadType table.ThreadType) *bridgev2.Portal {
	return &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: id},
		Metadata:  &metaid.PortalMetadata{ThreadType: threadType},
	}}
}

func TestMarkingUnreadIsTheOlderChatsRequest(t *testing.T) {
	task, status, err := markedUnreadRequest(unreadTestPortal("4242", table.GROUP_THREAD), true, time.UnixMilli(1700000000000))
	if err != nil || task != nil || status == nil {
		t.Fatalf("task %v status %v err %v", task, status, err)
	}
	if status.Endpoint != "mercury_change_read_status" || status.Form() != "ids[4242]=false&source&watermarkTimestamp&shouldSendReadReceipt" {
		t.Errorf("endpoint %q form %s", status.Endpoint, status.Form())
	}
}

func TestTakingTheUnreadMarkOffIsTheMarkReadTask(t *testing.T) {
	task, status, err := markedUnreadRequest(unreadTestPortal("4242", table.ONE_TO_ONE), false, time.UnixMilli(1700000000000))
	if err != nil || task == nil || status != nil {
		t.Fatalf("task %v status %v err %v", task, status, err)
	}
	payload, queue := task.Create()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	// LSOptimisticMarkThreadReadV2 of the web client: label 21, queued under the thread.
	if task.GetLabel() != "21" || queue != "4242" || string(data) != `{"thread_id":4242,"last_read_watermark_ts":1700000000000,"sync_group":1}` {
		t.Errorf("label %q queue %q payload %s", task.GetLabel(), queue, data)
	}
}

func TestEncryptedChatsAreNotMarkedUnread(t *testing.T) {
	for _, threadType := range []table.ThreadType{table.ENCRYPTED_OVER_WA_ONE_TO_ONE, table.ENCRYPTED_OVER_WA_GROUP} {
		portal := unreadTestPortal("100000000000002", threadType)
		if task, status, err := markedUnreadRequest(portal, true, time.Now()); task != nil || status != nil || !errors.Is(err, errUnreadEncryptedUnsupported) {
			t.Errorf("%v marked unread: task %v status %v err %v", threadType, task, status, err)
		}
		if task, status, err := markedUnreadRequest(portal, false, time.Now()); task != nil || status != nil || err != nil {
			t.Errorf("%v marked read is left to read receipts: task %v status %v err %v", threadType, task, status, err)
		}
	}
}

func TestMarkedUnreadIsHandled(t *testing.T) {
	var api bridgev2.NetworkAPI = testMetaClient()
	if _, ok := api.(bridgev2.MarkedUnreadHandlingNetworkAPI); !ok {
		t.Fatal("the client must take Matrix unread marks, or bridgev2 drops them")
	}
}

func TestOnlyPlainChatsDeclareMarkAsUnread(t *testing.T) {
	for name, caps := range map[string]*event.RoomFeatures{
		"dm":                   metaCaps,
		"dm with polls":        metaCapsPolls,
		"group":                metaCapsGroup,
		"group with polls":     metaCapsGroupPolls,
		"community":            metaCapsWithThreads,
		"community with polls": metaCapsWithThreadsPolls,
		"encrypted dm":         metaCapsWithE2E,
		"encrypted group":      metaCapsWithE2EGroup,
	} {
		encrypted := caps == metaCapsWithE2E || caps == metaCapsWithE2EGroup
		if caps.MarkAsUnread == encrypted {
			t.Errorf("%s: mark as unread = %v, want %v", name, caps.MarkAsUnread, !encrypted)
		}
	}
}
