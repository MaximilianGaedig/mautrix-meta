package connector

import (
	"testing"
	"time"

	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/messagix/presencestream"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/presence"
)

// Bridges send presence only. An offline contact whose last-active time Messenger shares used to
// get "last seen <time>" as a status message, a second and staler "last seen" next to the
// homeserver's own; the time goes to the activity log instead.
func TestOfflineContactHasNoStatusText(t *testing.T) {
	want := presence.State{Presence: event.PresenceOffline}
	u := presencestream.Update{UserID: 2, LastActiveTimeSeconds: 1758290000}
	if st := mapPresenceUpdate(&u); st != want {
		t.Errorf("stream update with a last-active time: got %+v, want %+v", st, want)
	}
	now := time.UnixMilli(1758290000000)
	offline := &table.LSDeleteThenInsertContactPresence{ContactId: 1, Status: 1, LastActiveTimestampMs: 1758280000000}
	if st := mapLSContactPresence(offline, now); st != want {
		t.Errorf("LS row with a last-active time: got %+v, want %+v", st, want)
	}
	expired := &table.LSDeleteThenInsertContactPresence{ContactId: 1, Status: 2, ExpirationTimestampMs: 1758289000000}
	if st := mapLSContactPresence(expired, now); st != want {
		t.Errorf("expired LS row: got %+v, want %+v", st, want)
	}
}
