package connector

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/jsontime"
	"go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/metaid"
)

var _ bridgev2.DisappearTimerChangingNetworkAPI = (*MetaClient)(nil)

// e2eeDisappearingCap is what an encrypted chat can be set to. Only send-based timers exist there, and the
// values are the ones WhatsApp's protocol has.
var e2eeDisappearingCap = &event.DisappearingTimerCapability{
	Types: []event.DisappearingType{event.DisappearingTypeAfterSend},
	Timers: []jsontime.Milliseconds{
		jsontime.MS(whatsmeow.DisappearingTimer24Hours),
		jsontime.MS(whatsmeow.DisappearingTimer7Days),
		jsontime.MS(whatsmeow.DisappearingTimer90Days),
	},
}

// e2eeDisappearingSetting is the setting for a timer from Matrix, or an error when encrypted chats don't have
// that timer.
func e2eeDisappearingSetting(timer time.Duration) (database.DisappearingSetting, error) {
	switch timer {
	case whatsmeow.DisappearingTimerOff:
		return database.DisappearingSetting{Type: event.DisappearingTypeNone}, nil
	case whatsmeow.DisappearingTimer24Hours, whatsmeow.DisappearingTimer7Days, whatsmeow.DisappearingTimer90Days:
		return database.DisappearingSetting{Type: event.DisappearingTypeAfterSend, Timer: timer}, nil
	}
	return database.DisappearingSetting{}, fmt.Errorf("encrypted chats can't disappear after %s", timer)
}

// HandleMatrixDisappearingTimer sets the disappearing message timer of an encrypted chat. Groups have the timer
// as a setting on WhatsApp's servers, which is set directly. A one-to-one chat has no such setting: its timer
// travels in the metadata of every message, which the bridge attaches to each message it sends, so the other
// side learns the new timer with the next message. Chats that aren't encrypted have no timer to set.
func (m *MetaClient) HandleMatrixDisappearingTimer(ctx context.Context, msg *bridgev2.MatrixDisappearingTimer) (bool, error) {
	meta := msg.Portal.Metadata.(*metaid.PortalMetadata)
	if !meta.ThreadType.IsWhatsApp() {
		return false, fmt.Errorf("%w: only encrypted chats have a disappearing message timer", bridgev2.ErrDisappearingTimerUnsupported)
	}
	setting, err := e2eeDisappearingSetting(msg.Content.Timer.Duration)
	if err != nil {
		return false, err
	}
	settingTS := time.UnixMilli(msg.Event.Timestamp)
	if jid := meta.JID(msg.Portal.ID); jid.Server == waTypes.GroupServer {
		if m.E2EEClient == nil {
			return false, ErrNotConnected
		}
		if err = m.E2EEClient.SetDisappearingTimer(ctx, jid, setting.Timer, settingTS); err != nil {
			return false, err
		}
	} else {
		zerolog.Ctx(ctx).Debug().Dur("timer", setting.Timer).
			Msg("Encrypted one-to-one chat timer will be sent with the next message")
	}
	meta.EphemeralSettingTimestamp = settingTS.Unix()
	msg.Portal.Disappear = setting
	return true, nil
}
