package connector

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mau.fi/util/jsontime"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

func timerRequest(threadType table.ThreadType, portalID string, server string, timer time.Duration) *bridgev2.MatrixDisappearingTimer {
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: networkid.PortalID(portalID)},
		Metadata:  &metaid.PortalMetadata{ThreadType: threadType, WhatsAppServer: server},
	}}
	msg := &bridgev2.MatrixDisappearingTimer{}
	msg.Portal = portal
	msg.Event = &event.Event{Timestamp: 1758290000123}
	msg.Content = &event.BeeperDisappearingTimer{Type: event.DisappearingTypeAfterSend, Timer: timerMS(timer)}
	return msg
}

func TestEncryptedDisappearingSettings(t *testing.T) {
	for _, timer := range []time.Duration{24 * time.Hour, 7 * 24 * time.Hour, 90 * 24 * time.Hour} {
		got, err := e2eeDisappearingSetting(timer)
		if err != nil || got.Timer != timer || got.Type != event.DisappearingTypeAfterSend {
			t.Errorf("%s: %+v %v", timer, got, err)
		}
	}
	if got, err := e2eeDisappearingSetting(0); err != nil || got.Type != event.DisappearingTypeNone || got.Timer != 0 {
		t.Errorf("off: %+v %v", got, err)
	}
	if _, err := e2eeDisappearingSetting(time.Hour); err == nil {
		t.Error("an hour is not a timer encrypted chats have")
	}
}

func TestEncryptedDMTimerIsStoredForTheNextMessage(t *testing.T) {
	m := testMetaClient()
	msg := timerRequest(table.ENCRYPTED_OVER_WA_ONE_TO_ONE, "100000000000002", "msgr", 7*24*time.Hour)
	ok, err := m.HandleMatrixDisappearingTimer(context.Background(), msg)
	if !ok || err != nil {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	meta := msg.Portal.Metadata.(*metaid.PortalMetadata)
	if msg.Portal.Disappear.Timer != 7*24*time.Hour || msg.Portal.Disappear.Type != event.DisappearingTypeAfterSend {
		t.Errorf("disappear = %+v", msg.Portal.Disappear)
	}
	if meta.EphemeralSettingTimestamp != 1758290000 {
		t.Errorf("setting timestamp = %d", meta.EphemeralSettingTimestamp)
	}
}

func TestEncryptedGroupTimerNeedsAConnection(t *testing.T) {
	m := testMetaClient()
	msg := timerRequest(table.ENCRYPTED_OVER_WA_GROUP, "120363000000000001", "g.us", 24*time.Hour)
	ok, err := m.HandleMatrixDisappearingTimer(context.Background(), msg)
	if ok || !errors.Is(err, ErrNotConnected) {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if msg.Portal.Disappear.Timer != 0 || msg.Portal.Metadata.(*metaid.PortalMetadata).EphemeralSettingTimestamp != 0 {
		t.Error("a timer that could not be set must not be saved")
	}
}

func TestUnsupportedTimersAndChatsAreRejected(t *testing.T) {
	m := testMetaClient()
	msg := timerRequest(table.ENCRYPTED_OVER_WA_ONE_TO_ONE, "100000000000002", "msgr", time.Hour)
	if ok, err := m.HandleMatrixDisappearingTimer(context.Background(), msg); ok || err == nil {
		t.Errorf("an hour: ok=%v err=%v", ok, err)
	}
	plain := timerRequest(table.ONE_TO_ONE, "4242", "", 24*time.Hour)
	ok, err := m.HandleMatrixDisappearingTimer(context.Background(), plain)
	if ok || !errors.Is(err, bridgev2.ErrDisappearingTimerUnsupported) {
		t.Errorf("a chat that isn't encrypted has no timer: ok=%v err=%v", ok, err)
	}
}

func TestOnlyEncryptedRoomsDeclareATimer(t *testing.T) {
	for name, caps := range map[string]*event.RoomFeatures{"e2ee dm": metaCapsWithE2E, "e2ee group": metaCapsWithE2EGroup} {
		if caps.DisappearingTimer == nil || !caps.DisappearingTimer.Supports(&event.BeeperDisappearingTimer{
			Type: event.DisappearingTypeAfterSend, Timer: timerMS(24 * time.Hour),
		}) {
			t.Errorf("%s: no 24 hour timer", name)
		}
	}
	for name, caps := range map[string]*event.RoomFeatures{"dm": metaCaps, "group": metaCapsGroup} {
		if caps.DisappearingTimer != nil {
			t.Errorf("%s declares a timer that can't be set", name)
		}
	}
}

func timerMS(d time.Duration) jsontime.Milliseconds { return jsontime.MS(d) }
