// mautrix-meta - A Matrix-Facebook Messenger and Instagram DM puppeting bridge.
// Copyright (C) 2026 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package igconnector

import (
	"context"
	"time"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/metaid"
	"go.mau.fi/mautrix-meta/pkg/presence"
)

// startPresence starts the presence manager. Instagram's own "Active now" is
// not bridged: how its web client asks for it is not known well enough to
// imitate (see deltaActivity for what is used instead), so all presence here
// comes from what contacts are seen doing.
func (ic *IGConnector) startPresence(ctx context.Context) {
	if !ic.Config.PresenceBridging {
		return
	}
	ic.presence = presence.NewManager(presence.Config{
		// Same as the Messenger connector: forward changes quickly.
		Debounce: 2 * time.Second,
	}, presence.GhostSender(ic.Bridge))
	log := ic.Bridge.Log.With().Str("component", "presence").Logger()
	go ic.presence.Run(log.WithContext(context.WithoutCancel(ctx)))
}

// deltaIsLive reports whether a delta is happening now rather than being
// replayed: after a reconnect Instagram re-sends everything since the stored
// sequence ID, up to the latest one it announced when connecting.
func deltaIsLive(catchingUpTo, seqID int64) bool {
	return catchingUpTo == 0 || seqID > catchingUpTo
}

// deltaActivity picks out who a delta shows doing something, and when:
// sending a message or reading ours. A read receipt carries the time of the
// message that was read, not of the reading, so it only counts while it is
// live; a message has its own time, and an old one is dropped by the manager.
func deltaActivity(d *slidetypes.Delta, live bool, now time.Time) (fbid int64, at time.Time, ok bool) {
	switch evt := d.Data.(type) {
	case *slidetypes.NewMessageEvent:
		if evt.Message == nil {
			return 0, time.Time{}, false
		}
		return evt.Message.SenderFBID, evt.Message.TimestampMS.Time, evt.Message.SenderFBID > 0
	case *slidetypes.AdminMessageEvent:
		if evt.Message == nil {
			return 0, time.Time{}, false
		}
		return evt.Message.SenderFBID, evt.Message.TimestampMS.Time, evt.Message.SenderFBID > 0
	case *slidetypes.ReadReceiptEvent:
		return evt.ReadReceipt.ParticipantFBID, now, live && evt.ReadReceipt.ParticipantFBID > 0
	default:
		return 0, time.Time{}, false
	}
}

// noteActivity marks a contact online for a while (presence.Manager.Activity):
// what someone does is the most accurate presence we have, and on Instagram
// the only one.
func (ic *IGClient) noteActivity(fbid int64, at time.Time) {
	if ic.Main.presence == nil || fbid <= 0 || metaid.MakeUserLoginID(fbid) == ic.UserLogin.ID {
		return
	}
	ic.Main.presence.Activity(string(metaid.MakeUserID(fbid)), at)
}

func (ic *IGClient) noteDeltaActivity(d *slidetypes.Delta) {
	if ic.Main.presence == nil {
		return
	}
	if fbid, at, ok := deltaActivity(d, deltaIsLive(ic.catchingUpTo, d.UQSeqID), time.Now()); ok {
		ic.noteActivity(fbid, at)
	}
}
