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

func TestNewJoinSFU(t *testing.T) {
	cc := &CallContext{}
	msg := cc.NewJoin(&JoinParams{Offer: "v=0\r\n", SFU: true, GroupThreadID: "123456", E2eeMandated: true})
	jr := msg.Body.JoinRequest
	if jr.ClientMediaMode != int32(MediaPathSFU) || jr.Offer == nil || jr.Offer.SDP == "" {
		t.Fatalf("SFU join: mode %d offer %v", jr.ClientMediaMode, jr.Offer)
	}
	if jr.E2eeEnforcement == nil || jr.E2eeEnforcement.PreventSFUMode {
		t.Fatalf("SFU join must not ask to prevent the SFU: %+v", jr.E2eeEnforcement)
	}
	jc := string(jr.AppMessages[0].Data)
	if !strings.Contains(jc, `"group_thread_id":"123456"`) || !strings.Contains(jc, `"peer_id":null`) {
		t.Fatalf("joining_context %s", jc)
	}
	p2p := string(cc.NewJoin(&JoinParams{PeerID: "42"}).Body.JoinRequest.AppMessages[0].Data)
	if !strings.Contains(p2p, `"peer_id":"42"`) || !strings.Contains(p2p, `"group_thread_id":null`) {
		t.Fatalf("P2P joining_context changed: %s", p2p)
	}
}

// TestNewE2eeKeyMessageHAR: our E2eeKey DATA_MESSAGE body encodes byte-for-byte like the web client's.
func TestNewE2eeKeyMessageHAR(t *testing.T) {
	for _, hm := range loadHARMessages(t) {
		dm := hm.Msg.Body.DataMessageRequest
		if hm.Dir != "send" || dm == nil || dm.Message == nil || dm.Message.Topic != TopicE2eeKey {
			continue
		}
		cc := &CallContext{SelfID: dm.Message.Sender}
		ours := cc.NewE2eeKeyMessage(dm.Message.Recipients[0], dm.Message.Data).Body.DataMessageRequest
		want, got := &writer{}, &writer{}
		want.structBegin()
		dm.encode(want)
		want.structEnd()
		got.structBegin()
		ours.encode(got)
		got.structEnd()
		if string(want.bytes()) != string(got.bytes()) {
			t.Fatalf("our E2eeKey DATA_MESSAGE body differs from the web client's")
		}
		return
	}
	t.Fatal("no sent E2eeKey message in the capture")
}

// TestCoplayReadyHAR: our post-JOIN coplay UPDATE and the SFU JOIN's coplay state match the web
// client's in the captured group call.
func TestCoplayReadyHAR(t *testing.T) {
	var join, update *Message
	for _, hm := range loadHARMessages(t) {
		if hm.Dir != "send" {
			continue
		}
		if hm.Msg.Body.JoinRequest != nil && join == nil {
			join = hm.Msg
		}
		if u := hm.Msg.Body.UpdateRequest; u != nil && u.Topic == "coplay" && update == nil {
			update = hm.Msg
		}
	}
	if join == nil || update == nil {
		t.Skip("capture has no SFU JOIN and coplay UPDATE")
	}
	st, _ := join.Body.JoinRequest.SyncPayload.StateStore.Get("coplay")
	ours := (&CallContext{}).NewJoin(&JoinParams{SFU: true, PeerID: "1"})
	ost, _ := ours.Body.JoinRequest.SyncPayload.StateStore.Get("coplay")
	if string(st.Data) != string(ost.Data) {
		t.Errorf("JOIN coplay %x, web client %x", ost.Data, st.Data)
	}
	want, got := &writer{}, &writer{}
	want.structBegin()
	stateSyncEncoder{m: update.Body.UpdateRequest, member: 30}.encode(want)
	want.structEnd()
	got.structBegin()
	stateSyncEncoder{m: (&CallContext{}).NewCoplayReady().Body.UpdateRequest, member: 30}.encode(got)
	got.structEnd()
	if string(want.bytes()) != string(got.bytes()) {
		t.Errorf("coplay UPDATE differs from the web client's:\n got %x\nwant %x", got.bytes(), want.bytes())
	}
}
