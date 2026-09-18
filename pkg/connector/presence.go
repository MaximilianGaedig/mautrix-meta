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

package connector

import (
	"context"
	"errors"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/presencestream"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
	"go.mau.fi/mautrix-meta/pkg/presence"
)

// LastSeenPrefix starts every status_msg the bridge sets, so clients can
// recognise and reformat it (e.g. "last seen 5 minutes ago").
const LastSeenPrefix = "last seen "

// maxPresenceThreads is how many recent 1:1 chats have their other
// participant subscribed as an additional presence contact.
const maxPresenceThreads = messagix.MaxPresenceContacts

// mapMetaPresence converts a Meta contact status to Matrix presence: active
// contacts are online, everyone else is offline with the exact last active
// time in status_msg ("last seen <RFC3339 UTC>") when Meta shares it.
func mapMetaPresence(active bool, lastActive, until time.Time) presence.State {
	if active {
		return presence.State{Presence: event.PresenceOnline, Until: until}
	}
	st := presence.State{Presence: event.PresenceOffline}
	if !lastActive.IsZero() {
		st.StatusMsg = LastSeenPrefix + lastActive.UTC().Truncate(time.Second).Format(time.RFC3339)
	}
	return st
}

// mapPresenceUpdate maps one update from the PresenceUnifiedJSON stream.
func mapPresenceUpdate(u *presencestream.Update) presence.State {
	return mapMetaPresence(u.IsActive(), u.LastActive(), time.Time{})
}

// LSPresenceStatus from the web client's LS schema.
const lsPresenceStatusActive = 2

// mapLSContactPresence maps a deleteThenInsertContactPresence LS row
// (presence_states table). Rows that already expired are treated as offline.
func mapLSContactPresence(row *table.LSDeleteThenInsertContactPresence, now time.Time) presence.State {
	var lastActive, until time.Time
	if row.LastActiveTimestampMs > 0 {
		lastActive = time.UnixMilli(row.LastActiveTimestampMs)
	}
	if row.ExpirationTimestampMs > 0 {
		until = time.UnixMilli(row.ExpirationTimestampMs)
	}
	active := row.Status == lsPresenceStatusActive && (until.IsZero() || now.Before(until))
	if !active && row.Status == lsPresenceStatusActive && lastActive.IsZero() {
		lastActive = until
	}
	return mapMetaPresence(active, lastActive, until)
}

// presenceTracker turns the stream's full and incremental publishes into
// per-user states. It remembers who is online so that users dropped from a
// full snapshot, or everyone when the stream dies, are marked offline instead
// of staying online forever.
type presenceTracker struct {
	lock   sync.Mutex
	online map[int64]time.Time // user ID -> last time confirmed active
}

func (pt *presenceTracker) applyPublish(pub *presencestream.Publish, self int64, now time.Time) map[int64]presence.State {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	if pt.online == nil {
		pt.online = make(map[int64]time.Time)
	}
	out := make(map[int64]presence.State, len(pub.PresenceUpdates))
	seen := make(map[int64]struct{}, len(pub.PresenceUpdates))
	for i := range pub.PresenceUpdates {
		u := &pub.PresenceUpdates[i]
		id := int64(u.UserID)
		if id <= 0 || id == self {
			continue
		}
		seen[id] = struct{}{}
		st := mapPresenceUpdate(u)
		if u.IsActive() {
			pt.online[id] = now
		} else {
			if st.StatusMsg == "" {
				if last, ok := pt.online[id]; ok {
					st = mapMetaPresence(false, last, time.Time{})
				}
			}
			delete(pt.online, id)
		}
		out[id] = st
	}
	if pub.IsFull() {
		for id, last := range pt.online {
			if _, ok := seen[id]; !ok {
				out[id] = mapMetaPresence(false, last, time.Time{})
				delete(pt.online, id)
			}
		}
	}
	return out
}

// applyLS records presence from LS rows, so that the stream's bookkeeping
// knows about online users that came from the LS path.
func (pt *presenceTracker) applyLS(id int64, st presence.State, now time.Time) {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	if pt.online == nil {
		pt.online = make(map[int64]time.Time)
	}
	if st.Presence == event.PresenceOnline {
		pt.online[id] = now
	} else {
		delete(pt.online, id)
	}
}

// closeAll marks every user that was online as offline, last seen at the
// time they were last confirmed active.
func (pt *presenceTracker) closeAll() map[int64]presence.State {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	out := make(map[int64]presence.State, len(pt.online))
	for id, last := range pt.online {
		out[id] = mapMetaPresence(false, last, time.Time{})
	}
	clear(pt.online)
	return out
}

// presenceContacts remembers the other participant of recent 1:1 chats.
type presenceContacts struct {
	lock    sync.Mutex
	threads map[int64]int64 // other user ID -> last activity ms
	sent    []int64
}

// add records 1:1 threads from a table and returns the new contact list if
// it changed.
func (pc *presenceContacts) add(tbl *table.LSTable, self int64) ([]int64, bool) {
	pc.lock.Lock()
	defer pc.lock.Unlock()
	changed := false
	for _, thread := range tbl.LSDeleteThenInsertThread {
		tt := thread.ThreadType
		if !tt.IsOneToOne() || tt.IsWhatsApp() || thread.ThreadKey <= 0 || thread.ThreadKey == self {
			continue
		}
		if pc.threads == nil {
			pc.threads = make(map[int64]int64)
		}
		if old, ok := pc.threads[thread.ThreadKey]; !ok || old < thread.LastActivityTimestampMs {
			pc.threads[thread.ThreadKey] = thread.LastActivityTimestampMs
			changed = true
		}
	}
	if !changed {
		return nil, false
	}
	ids := pc.sortedLocked()
	if len(ids) > maxPresenceThreads {
		for _, id := range ids[maxPresenceThreads:] {
			delete(pc.threads, id)
		}
		ids = ids[:maxPresenceThreads]
	}
	set := slices.Sorted(slices.Values(ids))
	if slices.Equal(set, pc.sent) {
		return nil, false
	}
	pc.sent = set
	return ids, true
}

// current returns the contact list, most recently active chat first.
func (pc *presenceContacts) current() []int64 {
	pc.lock.Lock()
	defer pc.lock.Unlock()
	return pc.sortedLocked()
}

func (pc *presenceContacts) sortedLocked() []int64 {
	ids := make([]int64, 0, len(pc.threads))
	for id := range pc.threads {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		ai, aj := pc.threads[ids[i]], pc.threads[ids[j]]
		if ai != aj {
			return ai > aj
		}
		return ids[i] < ids[j]
	})
	return ids
}

func (m *MetaConnector) startPresence(ctx context.Context) {
	if !m.Config.PresenceBridging {
		return
	}
	m.presence = presence.NewManager(presence.Config{
		// Meta's status flips are coarse already; forward them quickly.
		Debounce: 2 * time.Second,
	}, presence.GhostSender(m.Bridge))
	log := m.Bridge.Log.With().Str("component", "presence").Logger()
	go m.presence.Run(log.WithContext(context.WithoutCancel(ctx)))
}

func (m *MetaClient) presenceEnabled() bool {
	return m.Main.presence != nil && m.LoginMeta.Platform.IsMessenger()
}

func (m *MetaClient) selfFBID() int64 {
	return metaid.ParseUserLoginID(m.UserLogin.ID)
}

func (m *MetaClient) sendPresenceStates(states map[int64]presence.State) {
	for id, st := range states {
		m.Main.presence.Update(string(metaid.MakeUserID(id)), st)
	}
}

func (m *MetaClient) startPresenceStream(ctx context.Context) {
	if !m.presenceEnabled() || m.Client == nil {
		return
	}
	m.Client.SetPresenceContacts(m.presenceContacts.current())
	err := m.Client.StartPresenceStream(ctx)
	if errors.Is(err, messagix.ErrPresenceUnsupported) {
		zerolog.Ctx(ctx).Debug().Msg("Presence stream not supported on this platform")
	} else if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to start presence stream")
	}
}

func (m *MetaClient) handlePresencePublish(evt *messagix.PresenceEvent) {
	if !m.presenceEnabled() {
		return
	}
	m.sendPresenceStates(m.presenceTracker.applyPublish(evt.Publish, m.selfFBID(), time.Now()))
}

func (m *MetaClient) handlePresenceStreamClosed() {
	if !m.presenceEnabled() {
		return
	}
	m.sendPresenceStates(m.presenceTracker.closeAll())
}

// handleTablePresence handles LS presence rows and collects presence
// contacts from 1:1 threads. It must run before parseTable, which rewrites
// thread keys of WhatsApp-hybrid threads.
func (m *MetaClient) handleTablePresence(ctx context.Context, tbl *table.LSTable) {
	if !m.presenceEnabled() {
		return
	}
	if len(tbl.LSDeleteThenInsertContactPresence) > 0 || len(tbl.LSTruncatePresenceDatabase) > 0 {
		// Temporary: nothing is known to deliver these on the DGW socket yet,
		// so log them loudly enough to notice if they do show up.
		zerolog.Ctx(ctx).Debug().
			Any("contact_presence", tbl.LSDeleteThenInsertContactPresence).
			Any("truncate_presence", tbl.LSTruncatePresenceDatabase).
			Msg("Received LS presence rows")
	}
	self := m.selfFBID()
	now := time.Now()
	for _, row := range tbl.LSDeleteThenInsertContactPresence {
		if row.ContactId <= 0 || row.ContactId == self {
			continue
		}
		st := mapLSContactPresence(row, now)
		m.presenceTracker.applyLS(row.ContactId, st, now)
		m.Main.presence.Update(string(metaid.MakeUserID(row.ContactId)), st)
	}
	if ids, changed := m.presenceContacts.add(tbl, self); changed && m.Client != nil {
		m.Client.SetPresenceContacts(ids)
	}
}
