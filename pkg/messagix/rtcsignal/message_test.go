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
	"encoding/hex"
	"reflect"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix/dgw"
)

func TestJoinRoundTrip(t *testing.T) {
	msg := fakeJoin([]byte{0x00})
	payload, err := EncodePayload(msg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodePayload(payload, false)
	if err != nil {
		t.Fatal(err)
	}
	jr := got.Body.JoinRequest
	if got.Header.Type != TypeJoin || got.Body.FieldID != 1 || jr == nil {
		t.Fatalf("wrong message: %+v", got.Header)
	}
	if got.Header.SenderID != fakeCaller || got.Header.ClientStack != ClientStackZenon ||
		got.Header.ConferenceType != ConferenceTypeRoom || got.Header.ConferenceName != "" {
		t.Errorf("header mismatch: %+v", got.Header)
	}
	if jr.Offer.SDP != fakeOfferSDP || !reflect.DeepEqual(jr.UsersToCall, []string{fakeCallee}) ||
		!reflect.DeepEqual(jr.DeviceCapabilities, WebDeviceCapabilities) || !jr.MediaStatus[fakeTrackID] {
		t.Errorf("join body mismatch")
	}
	if len(jr.AppMessages) != 1 || jr.AppMessages[0].Topic != "joining_context" ||
		string(jr.AppMessages[0].Data) != fakeJoiningContext {
		t.Errorf("app message mismatch: %+v", jr.AppMessages)
	}
	if st, ok := jr.SyncPayload.StateStore.Get(TopicE2eeState); !ok || st.Version != 1 {
		t.Errorf("missing E2eeState")
	}
	if jr.SyncPayload.StateStore[0].Topic != "coplay" {
		t.Errorf("state store order not preserved")
	}
	if jr.E2eeEnforcement.Mode != E2eeMandated || jr.JoinMode == nil || *jr.JoinMode != 0 {
		t.Errorf("enforcement/join mode mismatch")
	}
	// A decoded client message must re-encode to the same bytes.
	again, err := EncodePayload(got)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again, payload) {
		t.Fatal("re-encoding changed the bytes")
	}
}

// goldenJoinHeader is the header of fakeJoin, cross-checked with an
// independent Thrift compact decoder that also parses real captured frames:
// type=0, conferenceName="", transactionId, retryCount=0, sequenceNumber=0,
// clientSessionId, conferenceType=15, clientStack=5, sender{id}, messageTags={}.
const goldenJoinHeader = "15001800180a3130303030303030303114004600180a313233343536373839304" +
	"51e550a2c180f31303030303030303030303030303100" + "2a0500"

func TestJoinHeaderGolden(t *testing.T) {
	w := &writer{}
	fakeJoin(nil).Header.encode(w)
	want, _ := hex.DecodeString(goldenJoinHeader)
	if !bytes.Equal(w.bytes(), want) {
		t.Fatalf("header bytes differ\ngot  %x\nwant %x", w.bytes(), want)
	}
}

// TestCallFlow replays the message sequence of a captured outgoing 1:1 call
// with fake data: JOIN, JOIN response, ICE trickle both ways, conference
// state pushes, the callee's answer in SERVER_MEDIA_UPDATE, client events,
// state-sync notify, and finally a hangup.
func TestCallFlow(t *testing.T) {
	var clientSeq, serverSeq int64
	client := func(m *Message) []byte {
		m.Header.SequenceNumber = clientSeq
		clientSeq++
		b, err := EncodePayload(m)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	server := func(m *Message) []byte {
		m.Header.SequenceNumber = serverSeq
		serverSeq++
		b, err := m.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		return (&Envelope{Payload: b, ServiceType: ServiceMWS}).MarshalResponse()
	}
	serverHeader := func(typ MessageType, txn string, status int32) Header {
		return Header{
			Type: typ, ConferenceName: fakeRoom, TransactionID: txn, ServerInfoData: fakeServerInfo,
			ResponseStatusCode: status, ConferenceType: ConferenceTypeRoom,
			ReceiverUserID: fakeCaller, ServerMsgTime: 1700000000000, MessageTags: []int32{},
		}
	}

	type step struct {
		fromServer bool
		payload    []byte
		check      func(*Message)
	}
	join := fakeJoin([]byte{0x00})
	steps := []step{
		{false, client(join), func(m *Message) {
			if m.Body.JoinRequest == nil {
				t.Error("expected join request")
			}
		}},
		{true, server(&Message{Header: serverHeader(TypeJoin, "1000000001", StatusOK), Body: Body{JoinResponse: &JoinResponse{
			Answer: &SessionDescription{}, Initiator: fakeCaller, MediaPath: MediaPathP2P, SelfSCTPNodeID: 1,
			StateStore: StateStore{{Topic: TopicE2eeState, State: State{Version: 1, Data: []byte{0}}}},
		}}}), func(m *Message) {
			jr := m.Body.JoinResponse
			if !m.Header.IsResponse() || jr == nil || jr.MediaPath != MediaPathP2P || jr.Initiator != fakeCaller {
				t.Errorf("bad join response: %+v", jr)
			}
			if m.Header.ConferenceName != fakeRoom {
				t.Error("conference name not assigned")
			}
		}},
		{false, client(&Message{Header: Header{Type: TypeIceCandidate, ConferenceName: fakeRoom, TransactionID: "1000000002",
			ServerInfoData: fakeServerInfo, ClientSessionID: fakeSessionID, ConferenceType: ConferenceTypeRoom,
			ClientStack: ClientStackZenon, SenderID: fakeCaller, MessageTags: []int32{}},
			Body: Body{IceCandidateRequest: &IceCandidateRequest{Candidates: []IceCandidate{{
				Candidate: "candidate:1 1 udp 2122194687 192.0.2.10 50000 typ host generation 0 ufrag FAKE network-id 1",
				SDPMid:    "0",
			}}}}}), func(m *Message) {
			if c := m.Body.IceCandidateRequest; c == nil || len(c.Candidates) != 1 || c.Candidates[0].SDPMid != "0" {
				t.Error("bad candidate")
			}
		}},
		{true, server(&Message{Header: serverHeader(TypeIceCandidate, "1000000002", StatusOK)}), func(m *Message) {
			if m.Body.FieldID != 0 || !m.Header.IsResponse() {
				t.Error("expected empty ICE ack")
			}
		}},
		{true, server(&Message{Header: serverHeader(TypeConferenceState, "5000000001", 0), Body: Body{ConferenceStateRequest: &ConferenceStateRequest{
			Version: 5,
			ParticipantStates: map[string]ParticipantState{
				fakeCaller: {State: StateConnected, UserCapabilities: fakeUserCapabilities, SCTPNodeID: 1},
				fakeCallee: {State: StateRinging},
			},
			AppMessages: []DataMessage{{Sender: fakeCaller, ShouldSendToAll: true, Topic: "collision_context_payload", Data: []byte(`{"peer_id":"` + fakeCallee + `"}`)}},
		}}}), func(m *Message) {
			cs := m.Body.ConferenceStateRequest
			if cs == nil || cs.ParticipantStates[fakeCallee].State != StateRinging || !cs.AppMessages[0].ShouldSendToAll {
				t.Errorf("bad conference state: %+v", cs)
			}
		}},
		{true, server(&Message{Header: func() Header {
			h := serverHeader(TypeServerMediaUpdate, "5000000002", 0)
			h.MessageTags = []int32{int32(TagInitialAnswerToP2PCaller)}
			return h
		}(), Body: Body{ServerMediaUpdateRequest: &ServerMediaUpdateRequest{
			FromVersion: 0, ToVersion: 2, Answer: &SessionDescription{SDP: fakeOfferSDP},
			MediaStatus:      map[string]TrackInfo{fakeTrackID: {Enabled: true, Owner: fakeCallee}},
			SDPOriginLocalID: fakeCallee, MediaPath: MediaPathP2P, MultipleVideoStreamsAllowed: true,
		}}}), func(m *Message) {
			smu := m.Body.ServerMediaUpdateRequest
			typ, sd := smu.RemoteSDP()
			if typ != "answer" || sd.SDP != fakeOfferSDP || smu.SDPOriginLocalID != fakeCallee || smu.MediaStatus[fakeTrackID].Owner != fakeCallee {
				t.Error("bad server media update")
			}
			if m.Header.MessageTags[0] != int32(TagInitialAnswerToP2PCaller) {
				t.Error("tag lost")
			}
		}},
		{false, client(NewResponse(&Message{Header: serverHeader(TypeServerMediaUpdate, "5000000002", 0)}, 0, fakeCaller, fakeSessionID,
			Body{ServerMediaUpdateResponse: &ServerMediaUpdateResponse{CurrentVersion: 2}})), func(m *Message) {
			if m.Header.ResponseStatusCode != StatusOK || m.Header.TransactionID != "5000000002" || m.Body.ServerMediaUpdateResponse.CurrentVersion != 2 {
				t.Error("bad SMU response")
			}
		}},
		{false, client(&Message{Header: Header{Type: TypeClientEvent, ConferenceName: fakeRoom, TransactionID: "1000000003", SenderID: fakeCaller, MessageTags: []int32{}},
			Body: Body{ClientEventRequest: &ClientEventRequest{Events: []ClientEvent{{Type: ClientEventMediaConnected}}}}}), func(m *Message) {
			if m.Body.ClientEventRequest.Events[0].Type != ClientEventMediaConnected {
				t.Error("bad client event")
			}
		}},
		{true, server(&Message{Header: serverHeader(TypeNotify, "5000000003", 0), Body: Body{NotifyRequest: &StateSyncMessage{
			Topic: "batched_notify", SyncPayload: &SyncPayload{StateStore: StateStore{{Topic: "coplay", State: State{Version: 4, Data: []byte{0}}}}},
		}}}), func(m *Message) {
			n := m.Body.NotifyRequest
			if n == nil || n.Topic != "batched_notify" || len(n.SyncPayload.StateStore) != 1 {
				t.Error("bad notify")
			}
		}},
		{false, client(&Message{Header: Header{Type: TypeHangup, ConferenceName: fakeRoom, TransactionID: "1000000004", SenderID: fakeCaller, MessageTags: []int32{}},
			Body: Body{HangupRequest: &HangupRequest{Reason: HangupHangupCall}}}), func(m *Message) {
			if m.Body.HangupRequest == nil || m.Body.HangupRequest.Reason != HangupHangupCall {
				t.Error("bad hangup")
			}
		}},
	}

	for i, st := range steps {
		// Wrap in DGW frames the way the gateway does: a data frame that
		// requires an ack, sometimes coalesced with an ack for the other
		// direction in the same websocket message.
		df := &dgw.DataFrame{StreamID: 0, Payload: st.payload, RequiresAck: true, AckID: uint16(i)}
		ws := df.MarshalAppend(nil)
		if i%2 == 0 {
			ws = (&dgw.AckFrame{StreamID: 0, AckID: uint16(i)}).MarshalAppend(ws)
		}
		events, err := DecodeWebsocketMessage(ws, st.fromServer)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if ev := events[0]; ev.Err != nil || ev.Message == nil {
			t.Fatalf("step %d: decode: %v", i, ev.Err)
		}
		if got := events[0].Frame.(*dgw.DataFrame); !got.RequiresAck || got.AckID != uint16(i) {
			t.Errorf("step %d: DGW data frame fields lost", i)
		}
		if i%2 == 0 {
			if ack, ok := events[1].Frame.(*dgw.AckFrame); !ok || ack.AckID != uint16(i) {
				t.Errorf("step %d: coalesced ack not decoded", i)
			}
		}
		st.check(events[0].Message)
	}
}

func TestUnknownBodyPassthrough(t *testing.T) {
	// A body member this package does not model (approvalRequest, 42) must
	// decode, keep its raw bytes and re-encode unchanged.
	w := &writer{}
	w.structBegin()
	w.fieldI32(2, 1)
	w.fieldStringList(3, TypeSet, []string{fakeCallee})
	w.structEnd()
	raw := w.bytes()
	msg := &Message{Header: Header{Type: TypeApproval, TransactionID: "1", MessageTags: []int32{}}, Body: Body{FieldID: 42, Raw: raw}}
	b, err := msg.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Unmarshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Body.Name() != "approvalRequest" || !bytes.Equal(got.Body.Raw, raw) {
		t.Fatalf("unknown body not preserved: %s", got.Body.Name())
	}
	b2, _ := got.Marshal()
	if !bytes.Equal(b, b2) {
		t.Fatal("passthrough changed bytes")
	}
}

func TestServerErrorEnvelope(t *testing.T) {
	env := (&Envelope{ServiceType: ServiceMWS, Error: "boom"}).MarshalResponse()
	if _, err := DecodePayload(env, true); err == nil {
		t.Fatal("expected error")
	}
	e, err := ParseEnvelope(env, true)
	if err != nil || e.Error != "boom" {
		t.Fatalf("bad envelope: %v %+v", err, e)
	}
}

func TestEstablishStreamFrame(t *testing.T) {
	// The rpsignaling stream is opened with "{}" and answered {"code":200}.
	out := (&dgw.EstablishStreamFrame{StreamID: 0, RawParameters: []byte(RPSignalingEstablishParam)}).MarshalAppend(nil)
	if !bytes.Equal(out, []byte{15, 0, 0, 2, 0, 0, '{', '}'}) {
		t.Fatalf("got % x", out)
	}
	resp := append([]byte{15, 0, 0, 12, 0, 0}, `{"code":200}`...)
	events, err := DecodeWebsocketMessage(resp, true)
	if err != nil || len(events) != 1 {
		t.Fatal(err)
	}
	if _, ok := events[0].Frame.(*dgw.EstablishStreamFrame); !ok {
		t.Fatalf("got %T", events[0].Frame)
	}
}
