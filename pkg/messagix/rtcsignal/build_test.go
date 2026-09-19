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
	"bytes"
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
)

// fakeRing is a server RingRequest shaped like the captured one: caller,
// otherParticipants = [callee], app messages (caller_client_session_id,
// collision_context_payload), the caller's offer, its mediaStatusEx,
// sdpOriginLocalId, mediaPath P2P, E2EE mandated, and a TURN relay.
func fakeRing() *Message {
	return &Message{
		Header: Header{
			Type: TypeRing, ConferenceName: fakeRoom, TransactionID: "16900000000000000001",
			ServerInfoData: fakeServerInfo, SequenceNumber: 1, ConferenceType: ConferenceTypeRoom,
			ReceiverUserID: fakeCallee, ServerMsgTime: 1700000000000,
		},
		Body: Body{RingRequest: &RingRequest{
			Caller:            fakeCaller,
			OtherParticipants: []string{fakeCallee},
			RingType:          RingGroupAudio,
			AppMessages: []DataMessage{
				{Topic: "caller_client_session_id", Data: []byte(fakeSessionID)},
				{Sender: fakeCallee, ShouldSendToAll: true, Topic: "collision_context_payload",
					Data: []byte(`{"group_thread_id":null,"peer_id":"` + fakeCaller + `","calling_tags":2}`)},
			},
			Offer:            &SessionDescription{SDP: fakeOfferSDP},
			MediaStatusEx:    map[string]TrackInfo{fakeTrackID: {Enabled: true, Owner: fakeCaller}},
			SDPOriginLocalID: fakeCaller,
			MediaPath:        MediaPathP2P,
			E2eeEnforcement:  &E2eeEnforcement{Mode: E2eeMandated},
			RelayInfo: &RelayInfo{
				Turns:        []TurnInfo{{IPv4: "192.0.2.10", IPv6: "2001:db8::10", UDPPort: 40003, SSLTCPPort: 8080, TLSPort: 443}},
				TurnUsername: "fakeuser",
				TurnPassword: "fakepass",
			},
		}},
	}
}

// TestRingOverMQTT parses a RING as it arrives on /t_rtc_multi (empty
// MqttThriftHeader, then the message) and checks the fields the bridge uses.
func TestRingOverMQTT(t *testing.T) {
	payload, err := EncodeMQTTPayload(nil, fakeRing())
	if err != nil {
		t.Fatal(err)
	}
	if payload[0] != 0x00 {
		t.Fatalf("MqttThriftHeader should be a single stop byte, got %x", payload[:1])
	}
	_, msg, err := DecodeMQTTPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	ring := msg.Body.RingRequest
	if msg.Header.Type != TypeRing || ring == nil || msg.Header.IsResponse() {
		t.Fatalf("not a ring request: %+v", msg.Header)
	}
	if ring.Caller != fakeCaller || ring.Offer.SDP != fakeOfferSDP || ring.MediaPath != MediaPathP2P ||
		ring.E2eeEnforcement.Mode != E2eeMandated || ring.SDPOriginLocalID != fakeCaller {
		t.Errorf("ring fields mismatch")
	}
	if ts := ring.MediaStatusEx[fakeTrackID]; !ts.Enabled || ts.Owner != fakeCaller {
		t.Errorf("mediaStatusEx: %+v", ts)
	}
	if ri := ring.RelayInfo; ri == nil || len(ri.Turns) != 1 || ri.Turns[0].IPv4 != "192.0.2.10" ||
		ri.Turns[0].IPv6 != "2001:db8::10" || ri.Turns[0].UDPPort != 40003 || ri.Turns[0].SSLTCPPort != 8080 ||
		ri.TurnUsername != "fakeuser" {
		t.Errorf("relay info: %+v", ri)
	}
	if msg.Header.ConferenceName != fakeRoom || msg.Header.ServerInfoData != fakeServerInfo {
		t.Errorf("header mismatch")
	}

	// The parent window's reply: status 200, RingResponse{0}, no sender.
	resp := NewRingResponse(msg, fakeSessionID, DeviceStatusOK)
	enc, err := EncodePayload(resp)
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodePayload(enc, false)
	if err != nil {
		t.Fatal(err)
	}
	if back.Header.ResponseStatusCode != StatusOK || back.Header.TransactionID != msg.Header.TransactionID ||
		back.Header.Has(20) || back.Body.RingResponse == nil || back.Body.RingResponse.DeviceStatus != DeviceStatusOK {
		t.Errorf("ring response mismatch: %+v", back.Header)
	}
}

// TestCalleeJoin checks the JOIN a callee sends to accept a ring in P2P
// mode: empty offer struct in field 1, the answer in field 14, no users to
// call, clientMediaMode P2P, and the header of the ring's conference.
func TestCalleeJoin(t *testing.T) {
	ring := fakeRing()
	cc := NewCallContext(fakeCallee, ring.Header.ConferenceName, ring.Header.ServerInfoData)
	const answerSDP = "v=0\r\ns=-\r\na=x-dtls-auth:AAAA\r\n"
	join := cc.NewJoin(&JoinParams{
		Answer: answerSDP, PeerID: fakeCaller, AudioTrackID: fakeTrackID,
		E2eeState: []byte{0x00}, E2eeMandated: true,
	})
	enc, err := EncodePayload(join)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodePayload(enc, false)
	if err != nil {
		t.Fatal(err)
	}
	h := got.Header
	if h.Type != TypeJoin || h.ConferenceName != fakeRoom || h.ServerInfoData != fakeServerInfo ||
		h.SenderID != fakeCallee || h.ClientStack != ClientStackZenon || h.ConferenceType != ConferenceTypeRoom ||
		h.ClientSessionID != cc.ClientSessionID || h.SequenceNumber != 0 || h.IsResponse() {
		t.Errorf("header mismatch: %+v", h)
	}
	if _, err = strconv.ParseUint(h.TransactionID, 10, 32); err != nil {
		t.Errorf("transaction id %q is not a 32-bit decimal", h.TransactionID)
	}
	jr := got.Body.JoinRequest
	if jr.Offer == nil || jr.Offer.SDP != "" {
		t.Errorf("callee JOIN must carry an empty offer struct")
	}
	if jr.Answer == nil || jr.Answer.SDP != answerSDP {
		t.Errorf("answer missing")
	}
	if len(jr.UsersToCall) != 0 || jr.ClientMediaMode != int32(MediaPathP2P) || jr.JoinMode == nil || *jr.JoinMode != 0 {
		t.Errorf("join params mismatch: users=%v mode=%d", jr.UsersToCall, jr.ClientMediaMode)
	}
	if !reflect.DeepEqual(jr.DeviceCapabilities, WebDeviceCapabilities) || !jr.MediaStatus[fakeTrackID] ||
		!jr.MediaStatusEx[fakeTrackID].Enabled || jr.UserCapabilities != WebUserCapabilities {
		t.Errorf("capabilities/media status mismatch")
	}
	if jr.E2eeEnforcement == nil || jr.E2eeEnforcement.Mode != E2eeMandated {
		t.Errorf("enforcement mismatch")
	}
	if st, ok := jr.SyncPayload.StateStore.Get(TopicE2eeState); !ok || !bytes.Equal(st.Data, []byte{0x00}) {
		t.Errorf("E2eeState missing")
	}
	if len(jr.AppMessages) != 1 || jr.AppMessages[0].Topic != JoiningContextTopic {
		t.Fatalf("joining_context missing")
	}
	var jc map[string]any
	if err = json.Unmarshal(jr.AppMessages[0].Data, &jc); err != nil {
		t.Fatal(err)
	}
	if jc["peer_id"] != fakeCaller || jc["server_info_data"] != fakeServerInfo || jc["calling_tags"] != float64(2) {
		t.Errorf("joining_context: %v", jc)
	}
	// Byte-stable after decoding, like every captured client message.
	again, _ := EncodePayload(got)
	if !bytes.Equal(again, enc) {
		t.Error("re-encoding changed the bytes")
	}

	// Follow-up requests share the session and count up.
	ice := cc.NewIceCandidates(IceCandidate{Candidate: "candidate:1 1 udp 1 192.0.2.1 9 typ host", SDPMid: "0"})
	hangup := cc.NewHangup(HangupHangupCall)
	if ice.Header.SequenceNumber != 1 || hangup.Header.SequenceNumber != 2 ||
		ice.Header.ClientSessionID != cc.ClientSessionID || ice.Header.TransactionID == join.Header.TransactionID {
		t.Errorf("sequence/session mismatch")
	}
	if hangup.Body.HangupRequest.Reason != HangupHangupCall {
		t.Errorf("hangup reason")
	}
}

// TestCallerJoin checks the caller form: offer in field 1, usersToCall,
// no conference yet.
func TestCallerJoin(t *testing.T) {
	cc := NewCallContext(fakeCaller, "", "")
	join := cc.NewJoin(&JoinParams{Offer: fakeOfferSDP, PeerID: fakeCallee, UsersToCall: []string{fakeCallee}, E2eeMandated: true})
	enc, err := EncodePayload(join)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodePayload(enc, false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Header.Has(5) || got.Header.ConferenceName != "" {
		t.Errorf("a first JOIN has no conference or serverInfoData")
	}
	jr := got.Body.JoinRequest
	if jr.Offer.SDP != fakeOfferSDP || jr.Answer != nil || !reflect.DeepEqual(jr.UsersToCall, []string{fakeCallee}) {
		t.Errorf("caller join mismatch")
	}
}

// TestDefaultResponses checks the acknowledgements the client sends for
// server pushes.
func TestDefaultResponses(t *testing.T) {
	cc := NewCallContext(fakeCallee, fakeRoom, fakeServerInfo)
	cs := &Message{Header: Header{Type: TypeConferenceState, TransactionID: "5", ConferenceName: fakeRoom},
		Body: Body{ConferenceStateRequest: &ConferenceStateRequest{Version: 7}}}
	resp := cc.Respond(cs, DefaultResponseBody(cs))
	if resp.Body.ConferenceStateResponse.CurrentVersion != 7 || resp.Header.TransactionID != "5" ||
		resp.Header.ResponseStatusCode != StatusOK || resp.Header.SenderID != fakeCallee {
		t.Errorf("conference state response mismatch")
	}
	smu := &Message{Header: Header{Type: TypeServerMediaUpdate, TransactionID: "6"},
		Body: Body{ServerMediaUpdateRequest: &ServerMediaUpdateRequest{FromVersion: 3, ToVersion: 4}}}
	if DefaultResponseBody(smu).ServerMediaUpdateResponse.CurrentVersion != 4 {
		t.Errorf("SMU response version")
	}
	notify := &Message{Header: Header{Type: TypeNotify}, Body: Body{NotifyRequest: &StateSyncMessage{
		Topic: "batched_notify", SyncPayload: &SyncPayload{StateStore: StateStore{{Topic: "config_engine", State: State{Version: 2}}}},
	}}}
	if nr := DefaultResponseBody(notify).NotifyResponse; nr.Topic != "config_engine" || nr.Version != 2 {
		t.Errorf("batched notify response: %+v", nr)
	}
	dm := &Message{Header: Header{Type: TypeDataMessage}, Body: Body{DataMessageRequest: &DataMessageRequest{}}}
	b := DefaultResponseBody(dm)
	w := &writer{}
	if err := b.encode(w); err != nil {
		t.Fatal(err)
	}
	// {19: {1: {}}} as the web client sends it.
	if want := []byte{0x0c, 0x26, 0x1b, 0x00, 0x00, 0x00}; !bytes.Equal(w.bytes(), want) {
		t.Errorf("data message response bytes %x, want %x", w.bytes(), want)
	}
	ice := &Message{Header: Header{Type: TypeIceCandidate}, Body: Body{IceCandidateRequest: &IceCandidateRequest{}}}
	if DefaultResponseBody(ice).FieldID != 0 {
		t.Errorf("ICE candidates are acked with an empty body")
	}
}

func TestJoinLabelsVideoTrack(t *testing.T) {
	cc := NewCallContext("100000000000001", "", "")
	msg := cc.NewJoin(&JoinParams{Offer: "v=0", PeerID: "2", AudioTrackID: "aud", VideoTrackID: "vid"})
	data, err := EncodePayload(msg)
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodePayload(data, false)
	if err != nil {
		t.Fatal(err)
	}
	ex := back.Body.JoinRequest.MediaStatusEx
	if ex["aud"].Label != TrackLabelAudio || ex["vid"].Label != TrackLabelVideo || !ex["vid"].Enabled {
		t.Fatalf("media status: %+v", ex)
	}
}

// TestJoinPreventsSFU: a JOIN asking to stay peer-to-peer carries e2eeEnforcement.preventSFUMode.
func TestJoinPreventsSFU(t *testing.T) {
	ring := fakeRing()
	cc := NewCallContext(fakeCallee, ring.Header.ConferenceName, ring.Header.ServerInfoData)
	for _, prevent := range []bool{true, false} {
		enc, err := EncodePayload(cc.NewJoin(&JoinParams{
			Answer: "v=0\r\n", PeerID: fakeCaller, AudioTrackID: fakeTrackID, E2eeMandated: true, PreventSFU: prevent,
		}))
		if err != nil {
			t.Fatal(err)
		}
		back, err := DecodePayload(enc, false)
		if err != nil {
			t.Fatal(err)
		}
		e := back.Body.JoinRequest.E2eeEnforcement
		if e == nil || e.Mode != E2eeMandated || e.PreventSFUMode != prevent {
			t.Errorf("preventSFUMode %v: got %+v", prevent, e)
		}
	}
}

// TestClientMediaUpdateRoundTrip: the CLIENT_MEDIA_UPDATE a client sends to turn its camera on
// (web client toThriftClientMediaUpdateRequest) survives encoding, and its response decodes.
func TestClientMediaUpdateRoundTrip(t *testing.T) {
	ring := fakeRing()
	cc := NewCallContext(fakeCallee, ring.Header.ConferenceName, ring.Header.ServerInfoData)
	tracks := map[string]TrackInfo{
		"audio-track": {Enabled: true, Label: TrackLabelAudio},
		"video-track": {Enabled: true, Label: TrackLabelVideo},
	}
	msg := cc.NewClientMediaUpdate(3, tracks, "v=0\r\n")
	enc, err := EncodePayload(msg)
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodePayload(enc, false)
	if err != nil {
		t.Fatal(err)
	}
	req := back.Body.ClientMediaUpdateRequest
	if back.Header.Type != TypeClientMediaUpdate || req == nil {
		t.Fatalf("not a client media update: %v", back.Header.Type)
	}
	if req.FromVersion != 3 || req.ToVersion != 3 || req.Offer == nil || req.Offer.SDP != "v=0\r\n" ||
		len(req.MediaUpdates) != 1 || !req.MediaUpdates[0].MediaStatus["video-track"] ||
		req.MediaUpdates[0].MediaStatusEx["video-track"].Label != TrackLabelVideo {
		t.Fatalf("request mismatch: %+v", req)
	}

	resp := NewResponse(back, 1, fakeCaller, "session", Body{ClientMediaUpdateResponse: &ClientMediaUpdateResponse{
		CurrentVersion: 3, Answer: &SessionDescription{SDP: "answer"}, SDPOriginLocalID: fakeCaller, MediaPath: MediaPathP2P,
	}})
	enc, err = EncodePayload(resp)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodePayload(enc, false)
	if err != nil {
		t.Fatal(err)
	}
	r := got.Body.ClientMediaUpdateResponse
	if r == nil || r.CurrentVersion != 3 || r.Answer == nil || r.Answer.SDP != "answer" || r.SDPOriginLocalID != fakeCaller || r.MediaPath != MediaPathP2P {
		t.Fatalf("response mismatch: %+v", r)
	}
}
