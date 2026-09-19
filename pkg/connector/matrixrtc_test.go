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
	"encoding/json"
	"testing"
	"time"

	"maunium.net/go/mautrix/event"
)

// Element X (ruma SessionMembershipData) silently ignores a membership missing any of these.
var elementXRequiredMemberFields = []string{
	"application", "call_id", "scope", "device_id", "foci_preferred", "focus_active", "expires",
}

func TestRTCMemberContentHasWhatElementXRequires(t *testing.T) {
	focus := &rtcTransport{Type: "livekit", LivekitServiceURL: "https://rtc.example/livekit/jwt"}
	content := rtcMemberContent(focus, "!room:example", "DEV", "@ghost:example", true, 0)
	// Round-trip through JSON: that is what goes on the wire.
	data, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err = json.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	for _, f := range elementXRequiredMemberFields {
		if _, ok := wire[f]; !ok {
			t.Errorf("membership lacks %q", f)
		}
	}
	if wire["application"] != "m.call" || wire["scope"] != "m.room" || wire["call_id"] != "" {
		t.Errorf("wrong call slot: %v %v %q", wire["application"], wire["scope"], wire["call_id"])
	}
	if wire["m.call.intent"] != "video" {
		t.Errorf("intent %v", wire["m.call.intent"])
	}
	foci := wire["foci_preferred"].([]any)[0].(map[string]any)
	if foci["type"] != "livekit" || foci["livekit_service_url"] != focus.LivekitServiceURL || foci["livekit_alias"] != "!room:example" {
		t.Errorf("foci_preferred %v", foci)
	}
	if _, ok := wire["created_ts"]; ok {
		t.Error("created_ts belongs only on renewals")
	}
}

func TestRTCStateKey(t *testing.T) {
	if got := rtcStateKey("@ghost:example", "DEV"); got != "_@ghost:example_DEV_m.call" {
		t.Fatalf("state key %q", got)
	}
}

func TestParseRTCMembership(t *testing.T) {
	now := time.Now().UnixMilli()
	mk := func(raw map[string]any, ts int64) *event.Event {
		return &event.Event{Timestamp: ts, Content: event.Content{Raw: raw}}
	}
	joined := map[string]any{
		"application": "m.call", "scope": "m.room", "call_id": "", "device_id": "PHONE",
		"expires": float64(14400000), "m.call.intent": "audio",
	}
	if m := parseRTCMembership(mk(joined, now)); !m.joined || m.device != "PHONE" || m.video {
		t.Errorf("joined: %+v", m)
	}
	if m := parseRTCMembership(mk(map[string]any{}, now)); m.joined {
		t.Error("{} is leaving")
	}
	if m := parseRTCMembership(mk(joined, now-5*3600*1000)); m.joined {
		t.Error("an expired membership isn't in the call")
	}
	video := map[string]any{"application": "m.call", "scope": "m.room", "device_id": "D", "m.call.intent": "video"}
	if m := parseRTCMembership(mk(video, now)); !m.joined || !m.video {
		t.Errorf("video: %+v", m)
	}
	if m := parseRTCMembership(mk(map[string]any{"application": "io.element.other"}, now)); m.joined {
		t.Error("other applications aren't calls")
	}
}
