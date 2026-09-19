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
	"context"
	"testing"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/messagix/callsignal"
	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
)

func ringFor(conference, serverInfo string) *rtcsignal.Message {
	return &rtcsignal.Message{
		Header: rtcsignal.Header{Type: rtcsignal.TypeRing, ConferenceName: conference, ServerInfoData: serverInfo, TransactionID: "1"},
		Body:   rtcsignal.Body{RingRequest: &rtcsignal.RingRequest{Caller: "100000000000001"}},
	}
}

// TestCallSignalRouting covers the per-login rules that need no network:
// RING retries of the active call are acked normally, a RING for another
// call while busy is answered IN_ANOTHER_CALL, and messages of unrelated
// conferences get the client's default reply.
func TestCallSignalRouting(t *testing.T) {
	cb := &callBridge{log: zerolog.Nop()}
	s := &callSession{cb: cb, conference: "ROOM:1", serverInfoData: "sid1", log: zerolog.Nop()}
	cb.active = s
	ctx := context.Background()

	if resp := cb.handleSignal(ctx, ringFor("ROOM:1", "sid1"), callsignal.TransportMQTT); resp != nil {
		t.Errorf("retry of the active ring should get the default ack, got %+v", resp.Body)
	}
	resp := cb.handleSignal(ctx, ringFor("ROOM:2", "sid2"), callsignal.TransportDGW)
	if resp == nil || resp.Body.RingResponse == nil || resp.Body.RingResponse.DeviceStatus != rtcsignal.DeviceStatusInAnotherCall {
		t.Fatalf("second call should be answered busy, got %+v", resp)
	}
	if resp.Header.TransactionID != "1" || resp.Header.ResponseStatusCode != rtcsignal.StatusOK {
		t.Errorf("busy response header mismatch")
	}
	other := &rtcsignal.Message{Header: rtcsignal.Header{Type: rtcsignal.TypeConferenceState, ConferenceName: "ROOM:9"},
		Body: rtcsignal.Body{ConferenceStateRequest: &rtcsignal.ConferenceStateRequest{Version: 1}}}
	if cb.handleSignal(ctx, other, callsignal.TransportDGW) != nil {
		t.Error("unrelated conference should get the default reply")
	}
	// Before the JOIN the session has no call context: default reply.
	ice := &rtcsignal.Message{Header: rtcsignal.Header{Type: rtcsignal.TypeIceCandidate, ConferenceName: "ROOM:1"},
		Body: rtcsignal.Body{IceCandidateRequest: &rtcsignal.IceCandidateRequest{Candidates: []rtcsignal.IceCandidate{{Candidate: "candidate:x", SDPMid: "0"}}}}}
	if cb.handleSignal(ctx, ice, callsignal.TransportDGW) != nil {
		t.Error("pre-join ICE should get the default reply")
	}
	if len(s.remoteCands) != 1 {
		t.Errorf("pre-join remote candidate not queued")
	}
	// After the JOIN, replies come from the call's own session.
	s.cc = rtcsignal.NewCallContext("100000000000002", "ROOM:1", "sid1")
	r := cb.handleSignal(ctx, other2("ROOM:1"), callsignal.TransportDGW)
	if r == nil || r.Header.ClientSessionID != s.cc.ClientSessionID || r.Body.ConferenceStateResponse == nil ||
		r.Body.ConferenceStateResponse.CurrentVersion != 4 {
		t.Errorf("joined reply mismatch: %+v", r)
	}
}

func other2(conf string) *rtcsignal.Message {
	return &rtcsignal.Message{Header: rtcsignal.Header{Type: rtcsignal.TypeConferenceState, ConferenceName: conf, TransactionID: "7"},
		Body: rtcsignal.Body{ConferenceStateRequest: &rtcsignal.ConferenceStateRequest{Version: 4}}}
}

func TestDismissHangupReason(t *testing.T) {
	cases := map[rtcsignal.DismissReason]event.CallHangupReason{
		rtcsignal.DismissAnsweredOnAnotherDevice: "answered_elsewhere",
		rtcsignal.DismissRejectedOnAnotherDevice: "user_busy",
		rtcsignal.DismissCallEnded:               event.CallHangupUserHangup,
	}
	for in, want := range cases {
		if got := dismissHangupReason(in); got != want {
			t.Errorf("%d: got %s, want %s", in, got, want)
		}
	}
}
