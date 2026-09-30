package connector

import (
	"testing"
	"time"

	"maunium.net/go/mautrix/event"
)

func TestMuteExpireTimeMS(t *testing.T) {
	if got := muteExpireTimeMS(&event.BeeperMuteEventContent{MutedUntil: -1}); got != -1 {
		t.Errorf("muted forever = %d", got)
	}
	if got := muteExpireTimeMS(&event.BeeperMuteEventContent{}); got != 0 {
		t.Errorf("unmuted = %d", got)
	}
	until := time.Now().Add(time.Hour).UnixMilli()
	if got := muteExpireTimeMS(&event.BeeperMuteEventContent{MutedUntil: until}); got != until {
		t.Errorf("muted for an hour = %d, want %d", got, until)
	}
}
