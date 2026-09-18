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

// Synthetic fixtures. Their structure mirrors a captured facebook.com
// 1:1 E2EE audio call (field layout, field order, value shapes), but every
// identifier, key, credential and address is fake.

const (
	fakeCaller    = "100000000000001"
	fakeCallee    = "100000000000002"
	fakeRoom      = "ROOM:1000000000000009"
	fakeSessionID = "1234567890"
	fakeTrackID   = "00000000-0000-4000-8000-000000000001"
	fakeStreamID  = "00000000-0000-4000-8000-0000000000aa"
	// fakeServerInfo decodes to {Region: "abc", CallKey: "AAAABBBBCCCCDDDD", Number: 42}.
	fakeServerInfo = "GANhYmMoEEFBQUFCQkJCQ0NDQ0REREQWVAA="
)

const fakeFingerprint = "sha-256 00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF"

// fakeOfferSDP has the shape of the web client's offer (audio sendrecv,
// video recvonly, SCTP data channel, one BUNDLE transport).
const fakeOfferSDP = "v=0\r\n" +
	"o=- 1111111111111111111 3 IN IP4 127.0.0.1\r\n" +
	"s=-\r\n" +
	"t=0 0\r\n" +
	"a=msid-semantic: WMS " + fakeStreamID + "\r\n" +
	"a=group:BUNDLE 0 1 2\r\n" +
	"m=audio 9 UDP/TLS/RTP/SAVPF 111 63 9 0 8 13 110 126\r\n" +
	"c=IN IP4 0.0.0.0\r\n" +
	"a=rtcp:9 IN IP4 0.0.0.0\r\n" +
	"a=setup:actpass\r\n" +
	"a=mid:0\r\n" +
	"a=msid:" + fakeStreamID + " " + fakeTrackID + "\r\n" +
	"a=sendrecv\r\n" +
	"a=ice-ufrag:FAKE\r\n" +
	"a=ice-pwd:FAKEFAKEFAKEFAKEFAKEFAKE\r\n" +
	"a=fingerprint:" + fakeFingerprint + "\r\n" +
	"a=ice-options:trickle fb-force-5245 renomination\r\n" +
	"a=rtcp-mux\r\n" +
	"a=rtpmap:111 opus/48000/2\r\n" +
	"a=fmtp:111 minptime=10;useinbandfec=1\r\n" +
	"m=video 9 UDP/TLS/RTP/SAVPF 108\r\n" +
	"c=IN IP4 0.0.0.0\r\n" +
	"a=setup:actpass\r\n" +
	"a=mid:1\r\n" +
	"a=recvonly\r\n" +
	"a=ice-ufrag:FAKE\r\n" +
	"a=ice-pwd:FAKEFAKEFAKEFAKEFAKEFAKE\r\n" +
	"a=fingerprint:" + fakeFingerprint + "\r\n" +
	"a=rtcp-mux\r\n" +
	"a=rtpmap:108 VP8/90000\r\n" +
	"m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n" +
	"c=IN IP4 0.0.0.0\r\n" +
	"a=setup:actpass\r\n" +
	"a=mid:2\r\n" +
	"a=ice-ufrag:FAKE\r\n" +
	"a=ice-pwd:FAKEFAKEFAKEFAKEFAKEFAKE\r\n" +
	"a=fingerprint:" + fakeFingerprint + "\r\n" +
	"a=sctp-port:5000\r\n" +
	"a=max-message-size:262144\r\n"

const fakeUserCapabilities = `{"AddParticipantEnabled":false,"GROUP_COWATCH":true,"MultipleVideoStreamsAllowed":true,"MW_AV_ESCALATION":true,"canApproveCollaborationSpaceJoinRequests":true,"cowatch":true,"screen_sharing":false,"sctpSecondPc":false}`

const fakeJoiningContext = `{"call_trigger":null,"callable_post_id":null,"calling_tags":2,"group_thread_id":null,"ig_thread_id":null,"link_url":null,"live_broadcast_id":null,"meeting_id":null,"peer_id":"` + fakeCallee + `","server_info_data":"` + fakeServerInfo + `"}`

func int32p(v int32) *int32 { return &v }

// fakeJoin builds the caller's JOIN as the web client sends it.
func fakeJoin(e2eeState []byte) *Message {
	return &Message{
		Header: Header{
			Type:            TypeJoin,
			TransactionID:   "1000000001",
			ClientSessionID: fakeSessionID,
			ConferenceType:  ConferenceTypeRoom,
			ClientStack:     ClientStackZenon,
			SenderID:        fakeCaller,
			MessageTags:     []int32{},
		},
		Body: Body{JoinRequest: &JoinRequest{
			Offer:              &SessionDescription{SDP: fakeOfferSDP},
			DeviceCapabilities: WebDeviceCapabilities,
			UsersToCall:        []string{fakeCallee},
			MediaStatus:        map[string]bool{fakeTrackID: true},
			UserCapabilities:   fakeUserCapabilities,
			AppMessages: []DataMessage{{
				TopicDeprecated: "joining_context",
				Topic:           "joining_context",
				Data:            []byte(fakeJoiningContext),
			}},
			MediaStatusEx: map[string]TrackInfo{fakeTrackID: {Enabled: true}},
			SyncPayload: &SyncPayload{StateStore: StateStore{
				{Topic: "coplay", State: State{Version: 1, Data: []byte{0x15, 0x00}}},
				{Topic: TopicE2eeState, State: State{Version: 1, Data: e2eeState}},
			}},
			E2eeEnforcement: &E2eeEnforcement{Mode: E2eeMandated},
			ClientMediaMode: 2,
			JoinMode:        int32p(0),
		}},
	}
}
