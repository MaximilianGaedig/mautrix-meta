package connector

import (
	"testing"

	"maunium.net/go/mautrix/event"
)

func TestRoomsDeclareLocationSupport(t *testing.T) {
	for name, caps := range map[string]*event.RoomFeatures{
		"dm":    metaCaps,
		"group": metaCapsGroup,
		"e2ee":  metaCapsWithE2E,
	} {
		if caps.LocationMessage != event.CapLevelPartialSupport {
			t.Errorf("%s: location = %v, want partial (sent as text with a map link)", name, caps.LocationMessage)
		}
	}
}
