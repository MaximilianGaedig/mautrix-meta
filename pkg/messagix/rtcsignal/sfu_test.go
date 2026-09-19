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

package rtcsignal

import (
	"strings"
	"testing"
)

func TestSessionDescriptionUpdateRoundTrip(t *testing.T) {
	in := &ServerMediaUpdateRequest{
		FromVersion: 3,
		ToVersion:   4,
		MediaPath:   MediaPathSFU,
		Update: &SessionDescriptionUpdate{Media: map[int32]MediaDescriptionUpdate{
			1: {Body: "m=video 9 UDP/TLS/RTP/SAVPF 96\r\na=inactive\r\na=mid:1\r\n", MID: "1"},
			3: {Body: "m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=sendonly\r\na=mid:3\r\n", MSID: "u:c:s t", MID: "3"},
		}},
	}
	w := &writer{}
	w.structBegin()
	in.encode(w)
	w.structEnd()
	var out ServerMediaUpdateRequest
	if err := out.decode(&reader{b: w.bytes()}); err != nil {
		t.Fatal(err)
	}
	if out.Update == nil || len(out.Update.Media) != 2 || out.Update.Media[3] != in.Update.Media[3] {
		t.Fatalf("update did not round-trip: %+v", out.Update)
	}
	if got := out.Update.Indexes(); got[0] != 1 || got[1] != 3 {
		t.Fatalf("indexes %v", got)
	}

	jr := &JoinResponse{MediaPath: MediaPathSFU, GroupsOfUsers: []GroupOfUsers{{Users: []string{"1", "2"}, AliasID: "a"}}}
	w = &writer{}
	w.structBegin()
	jr.encode(w)
	w.structEnd()
	var jout JoinResponse
	if err := jout.decode(&reader{b: w.bytes()}); err != nil {
		t.Fatal(err)
	}
	if len(jout.GroupsOfUsers) != 1 || jout.GroupsOfUsers[0].AliasID != "a" || len(jout.GroupsOfUsers[0].Users) != 2 {
		t.Fatalf("groupsOfUsers did not round-trip: %+v", jout.GroupsOfUsers)
	}
}

// TestSFUDeltaHAR checks the captured SFU call (RTCSIGNAL_HAR, e.g. the accept-popup capture): the
// PARTICIPANT_ADDED update carries a new sendonly audio m-section owned by the remote user, and the
// JoinResponse is on the SFU path with our SCTP node id.
func TestSFUDeltaHAR(t *testing.T) {
	msgs := loadHARMessages(t)
	var sawDelta, sawJoin bool
	for _, hm := range msgs {
		b := hm.Msg.Body
		if jr := b.JoinResponse; jr != nil && hm.Dir == "receive" && jr.MediaPath == MediaPathSFU {
			sawJoin = true
			if jr.SelfSCTPNodeID == 0 {
				t.Error("SFU JoinResponse without selfSctpNodeId")
			}
		}
		smu := b.ServerMediaUpdateRequest
		if smu == nil || smu.Update == nil {
			continue
		}
		sawDelta = true
		for _, idx := range smu.Update.Indexes() {
			m := smu.Update.Media[idx]
			t.Logf("delta m-line %d mid=%q msid=%q first=%q", idx, m.MID, m.MSID, strings.SplitN(m.Body, "\r\n", 2)[0])
			if !strings.HasPrefix(m.Body, "m=") {
				t.Errorf("m-line %d body is not an m-section", idx)
			}
		}
		var owners int
		for _, ti := range smu.MediaStatus {
			if ti.Owner != "" {
				owners++
			}
		}
		if owners == 0 {
			t.Error("delta SMU media status names no track owner")
		}
	}
	if !sawDelta || !sawJoin {
		t.Fatalf("capture lacks an SFU join (%t) or a delta SMU (%t)", sawJoin, sawDelta)
	}
}
