package connector

import (
	"testing"
	"time"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

func TestLiveLocationStartParts(t *testing.T) {
	row := &table.LSUpsertLiveLocationSharer{
		ThreadKey: 1, Sender: 2, Latitude: 52.5, Longitude: 13.4,
		StartTimestampMS: 1_700_000_000_000, EndTimestampMS: 1_700_000_000_000 + 3_600_000,
	}
	parts := liveLocationStartParts("@meta_2:example.org", row)
	if len(parts) != 2 {
		t.Fatalf("%d parts", len(parts))
	}
	info, beacon := parts[0], parts[1]
	if info.Type != event.StateUnstableBeaconInfo || info.StateKey == nil || *info.StateKey != "@meta_2:example.org" {
		t.Errorf("beacon_info part = %+v", info)
	}
	if info.Extra["live"] != true || info.Extra["timeout"] != int64(3_600_000) {
		t.Errorf("beacon_info content = %v", info.Extra)
	}
	if info.Content == nil || info.Content.GeoURI == "" {
		t.Error("the static location is the backfill fallback")
	}
	if beacon.Type != event.EventUnstableBeacon || !beacon.ReferencesPrevious || beacon.Content != nil {
		t.Errorf("beacon part = %+v", beacon)
	}
	if loc := beacon.Extra["org.matrix.msc3488.location"].(map[string]any); loc["uri"] != "geo:52.500000,13.400000" {
		t.Errorf("beacon location = %v", loc)
	}
}

func TestLiveLocationTimeout(t *testing.T) {
	if got := liveLocationTimeout(&table.LSUpsertLiveLocationSharer{StartTimestampMS: 1000}); got != defaultLiveLocationTimeout {
		t.Errorf("a share without an end lasts %v", got)
	}
}

func TestLiveLocationUpdatePart(t *testing.T) {
	part := liveLocationUpdatePart("$info", &table.LSUpsertLiveLocationSharer{Latitude: 1, Longitude: 2}, time.UnixMilli(5))
	rel := part.Extra["m.relates_to"].(map[string]any)
	if part.Content != nil || rel["event_id"] != id.EventID("$info") || part.Extra["org.matrix.msc3488.ts"] != int64(5) {
		t.Errorf("update part = %+v", part)
	}
	if liveLocationStartID(1, 2, 3) == liveLocationStartID(1, 2, 4) {
		t.Error("each share has its own start")
	}
}
