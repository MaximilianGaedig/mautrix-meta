package msgconv

import (
	"context"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/metaid"
	"go.mau.fi/mautrix-meta/pkg/msgconv/textfmt"
)

func encryptedPortal(disappear time.Duration, settingTS int64) *bridgev2.Portal {
	return &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: "100000000000002@msgr"},
		Disappear: database.DisappearingSetting{Type: event.DisappearingTypeAfterSend, Timer: disappear},
		Metadata:  &metaid.PortalMetadata{EphemeralSettingTimestamp: settingTS},
	}}
}

func sendText(t *testing.T, portal *bridgev2.Portal) *ephemeralSent {
	t.Helper()
	mc := &MessageConverter{HTMLParser: textfmt.NewMatrixParser(nil)}
	_, meta, err := mc.ToWhatsApp(context.Background(), &event.Event{Type: event.EventMessage},
		&event.MessageEventContent{MsgType: event.MsgText, Body: "hi"}, portal, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	setting := meta.GetChatEphemeralSetting()
	if setting == nil {
		return nil
	}
	return &ephemeralSent{expiration: setting.GetEphemeralExpiration(), ts: setting.GetEphemeralSettingTimestamp()}
}

type ephemeralSent struct {
	expiration uint32
	ts         int64
}

func TestEncryptedMessagesCarryTheTimer(t *testing.T) {
	got := sendText(t, encryptedPortal(24*time.Hour, 1758290000))
	if got == nil || got.expiration != 86400 || got.ts != 1758290000 {
		t.Errorf("setting = %+v", got)
	}
}

func TestEncryptedMessagesCarryTheTimerBeingTurnedOff(t *testing.T) {
	got := sendText(t, encryptedPortal(0, 1758290500))
	if got == nil || got.expiration != 0 || got.ts != 1758290500 {
		t.Errorf("a chat whose timer was turned off must say so: %+v", got)
	}
}

func TestEncryptedMessagesOfChatsThatNeverHadATimerCarryNoSetting(t *testing.T) {
	if got := sendText(t, encryptedPortal(0, 0)); got != nil {
		t.Errorf("setting = %+v", got)
	}
}
