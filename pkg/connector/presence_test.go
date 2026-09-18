package connector

import (
	"testing"
	"time"

	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/messagix/presencestream"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/presence"
)

func upd(id int64, active bool, lastActive int64) presencestream.Update {
	u := presencestream.Update{UserID: presencestream.FlexInt64(id), LastActiveTimeSeconds: presencestream.FlexInt64(lastActive)}
	if active {
		u.PresenceStatus = presencestream.StatusActive
	}
	return u
}

func TestMapPresenceUpdate(t *testing.T) {
	a := upd(1, true, 1758290000)
	if st := mapPresenceUpdate(&a); st.Presence != event.PresenceOnline || st.StatusMsg != "" || !st.Until.IsZero() {
		t.Errorf("active: got %+v", st)
	}
	b := upd(2, false, 1758290000)
	if st := mapPresenceUpdate(&b); st.Presence != event.PresenceOffline || st.StatusMsg != "last seen 2025-09-19T13:53:20Z" {
		t.Errorf("inactive with time: got %+v", st)
	}
	c := upd(3, false, 0)
	if st := mapPresenceUpdate(&c); st.Presence != event.PresenceOffline || st.StatusMsg != "" {
		t.Errorf("inactive without time: got %+v", st)
	}
}

func TestMapLSContactPresence(t *testing.T) {
	now := time.UnixMilli(1758290000000)
	active := &table.LSDeleteThenInsertContactPresence{ContactId: 1, Status: 2, LastActiveTimestampMs: 1758289990000, ExpirationTimestampMs: 1758290060000}
	if st := mapLSContactPresence(active, now); st.Presence != event.PresenceOnline || !st.Until.Equal(time.UnixMilli(1758290060000)) {
		t.Errorf("active: got %+v", st)
	}
	expired := &table.LSDeleteThenInsertContactPresence{ContactId: 1, Status: 2, ExpirationTimestampMs: 1758289000000}
	if st := mapLSContactPresence(expired, now); st.Presence != event.PresenceOffline || st.StatusMsg != "last seen 2025-09-19T13:36:40Z" {
		t.Errorf("expired: got %+v", st)
	}
	offline := &table.LSDeleteThenInsertContactPresence{ContactId: 1, Status: 1, LastActiveTimestampMs: 1758280000000}
	if st := mapLSContactPresence(offline, now); st.Presence != event.PresenceOffline || st.StatusMsg != "last seen 2025-09-19T11:06:40Z" {
		t.Errorf("offline: got %+v", st)
	}
}

func TestTrackerFullPublishDropsMissing(t *testing.T) {
	var pt presenceTracker
	t0 := time.Unix(1758290000, 0)
	out := pt.applyPublish(&presencestream.Publish{
		PublishType:     presencestream.PublishTypeFull,
		PresenceUpdates: []presencestream.Update{upd(1, true, 0), upd(2, true, 0), upd(99, true, 0)},
	}, 99, t0)
	if len(out) != 2 || out[1].Presence != event.PresenceOnline || out[2].Presence != event.PresenceOnline {
		t.Fatalf("first publish: %+v", out)
	}
	// Incremental: user 1 goes inactive without a timestamp, so the last time
	// we saw them active is used.
	t1 := t0.Add(time.Minute)
	out = pt.applyPublish(&presencestream.Publish{
		PublishType:     presencestream.PublishTypeIncremental,
		PresenceUpdates: []presencestream.Update{upd(1, false, 0)},
	}, 99, t1)
	if st := out[1]; st.Presence != event.PresenceOffline || st.StatusMsg != "last seen 2025-09-19T13:53:20Z" {
		t.Errorf("incremental: %+v", out)
	}
	if _, ok := out[2]; ok {
		t.Errorf("incremental publish must not touch other users: %+v", out)
	}
	// Full snapshot without user 2 marks them offline.
	out = pt.applyPublish(&presencestream.Publish{PublishType: presencestream.PublishTypeFull}, 99, t1)
	if st := out[2]; st.Presence != event.PresenceOffline || st.StatusMsg != "last seen 2025-09-19T13:53:20Z" {
		t.Errorf("full: %+v", out)
	}
}

func TestTrackerCloseAll(t *testing.T) {
	var pt presenceTracker
	t0 := time.Unix(1758290000, 0)
	pt.applyPublish(&presencestream.Publish{PresenceUpdates: []presencestream.Update{upd(5, true, 0)}}, 0, t0)
	pt.applyLS(6, presence.State{Presence: event.PresenceOnline}, t0)
	out := pt.closeAll()
	if len(out) != 2 || out[5].Presence != event.PresenceOffline || out[6].StatusMsg != "last seen 2025-09-19T13:53:20Z" {
		t.Errorf("closeAll: %+v", out)
	}
	if out = pt.closeAll(); len(out) != 0 {
		t.Errorf("second closeAll: %+v", out)
	}
}

func TestPresenceContacts(t *testing.T) {
	var pc presenceContacts
	tbl := &table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{
		{ThreadKey: 10, ThreadType: table.ONE_TO_ONE, LastActivityTimestampMs: 100},
		{ThreadKey: 11, ThreadType: table.ONE_TO_ONE, LastActivityTimestampMs: 300},
		{ThreadKey: 12, ThreadType: table.GROUP_THREAD, LastActivityTimestampMs: 500},
		{ThreadKey: 13, ThreadType: table.ENCRYPTED_OVER_WA_ONE_TO_ONE, LastActivityTimestampMs: 500},
		{ThreadKey: 99, ThreadType: table.ONE_TO_ONE, LastActivityTimestampMs: 500},
	}}
	ids, changed := pc.add(tbl, 99)
	if !changed || len(ids) != 2 || ids[0] != 11 || ids[1] != 10 {
		t.Fatalf("got %v %v", ids, changed)
	}
	// Newer activity reorders but the set is the same, so nothing to resend.
	tbl.LSDeleteThenInsertThread = []*table.LSDeleteThenInsertThread{{ThreadKey: 10, ThreadType: table.ONE_TO_ONE, LastActivityTimestampMs: 400}}
	if ids, changed = pc.add(tbl, 99); changed {
		t.Errorf("same set reported as changed: %v", ids)
	}
	if cur := pc.current(); len(cur) != 2 || cur[0] != 10 {
		t.Errorf("current: %v", cur)
	}
}
