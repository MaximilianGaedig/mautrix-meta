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

import "fmt"

// MessageType is multiway_MessageType, carried in RtcMessageHeader.type. A
// request and its response share the type and the transaction id.
type MessageType int32

const (
	TypeJoin               MessageType = 0
	TypeServerMediaUpdate  MessageType = 1
	TypeHangup             MessageType = 2
	TypeIceCandidate       MessageType = 3
	TypeRing               MessageType = 4
	TypeDismiss            MessageType = 5
	TypeConferenceState    MessageType = 6
	TypeAddParticipants    MessageType = 7
	TypeSubscription       MessageType = 8
	TypeClientMediaUpdate  MessageType = 9
	TypeDataMessage        MessageType = 10
	TypeRemoveParticipants MessageType = 11
	TypePing               MessageType = 18
	TypeUpdate             MessageType = 20
	TypeNotify             MessageType = 21
	TypeConnect            MessageType = 22
	TypeClientEvent        MessageType = 23
	TypeUnsubscribe        MessageType = 25
	TypeApproval           MessageType = 26
	TypeTransfer           MessageType = 27
	TypeWakeup             MessageType = 28
)

var messageTypeNames = map[MessageType]string{
	TypeJoin: "JOIN", TypeServerMediaUpdate: "SERVER_MEDIA_UPDATE", TypeHangup: "HANGUP",
	TypeIceCandidate: "ICE_CANDIDATE", TypeRing: "RING", TypeDismiss: "DISMISS",
	TypeConferenceState: "CONFERENCE_STATE", TypeAddParticipants: "ADD_PARTICIPANTS",
	TypeSubscription: "SUBSCRIPTION", TypeClientMediaUpdate: "CLIENT_MEDIA_UPDATE",
	TypeDataMessage: "DATA_MESSAGE", TypeRemoveParticipants: "REMOVE_PARTICIPANTS",
	TypePing: "PING", TypeUpdate: "UPDATE", TypeNotify: "NOTIFY", TypeConnect: "CONNECT",
	TypeClientEvent: "CLIENT_EVENT", TypeUnsubscribe: "UNSUBSCRIBE", TypeApproval: "APPROVAL",
	TypeTransfer: "TRANSFER", TypeWakeup: "WAKEUP",
}

func (t MessageType) String() string {
	if s, ok := messageTypeNames[t]; ok {
		return s
	}
	return fmt.Sprintf("MessageType(%d)", int32(t))
}

// ConferenceType is multiway_ConferenceType. Messenger 1:1 and group calls
// from the web client both use ROOM.
type ConferenceType int32

const (
	ConferenceTypeUnknown     ConferenceType = 0
	ConferenceTypeIGVideoCall ConferenceType = 9
	ConferenceTypeRoom        ConferenceType = 15
)

// ClientStack is fbwebrtc_ClientStack. The web client sends ZENON.
type ClientStack int32

const (
	ClientStackRsysX ClientStack = 1
	ClientStackZenon ClientStack = 5
)

// MessageTag is multiway_MessageTag (RtcMessageHeader.messageTags).
type MessageTag int32

const (
	TagPrAnswer                     MessageTag = 1001
	TagInitialAnswerToP2PCaller     MessageTag = 1002
	TagFirstRemoteAlertedForInit    MessageTag = 2001
	TagFirstRemoteAnsweredForInit   MessageTag = 2002
	TagKeepAlivePing                MessageTag = 3001
	TagIsInitiator                  MessageTag = 4001
	TagPregenSDP                    MessageTag = 4002
	TagRequestClientFullRenegMWS    MessageTag = 1008
	TagParticipantAdded             MessageTag = 1009
	TagParticipantRemoved           MessageTag = 1010
	TagContainPendingApprovalPartic MessageTag = 2003
)

// ParticipantCallState is multiway_ParticipantCallState.
type ParticipantCallState int32

const (
	StateUnknown           ParticipantCallState = 0
	StateDisconnected      ParticipantCallState = 1
	StateNoAnswer          ParticipantCallState = 2
	StateRejected          ParticipantCallState = 3
	StateUnreachable       ParticipantCallState = 4
	StateConnectionDropped ParticipantCallState = 5
	StateContacting        ParticipantCallState = 6
	StateRinging           ParticipantCallState = 7
	StateConnecting        ParticipantCallState = 8
	StateConnected         ParticipantCallState = 9
	StateInAnotherCall     ParticipantCallState = 11
)

// HangupReason is multiway_HangupReason.
type HangupReason int32

const (
	HangupIgnoreCall        HangupReason = 0
	HangupHangupCall        HangupReason = 1
	HangupNoAnswerTimeout   HangupReason = 2
	HangupClientError       HangupReason = 3
	HangupInAnotherCall     HangupReason = 4
	HangupWebRTCError       HangupReason = 9
	HangupConnectionDropped HangupReason = 10
)

// DismissReason is multiway_DismissReason.
type DismissReason int32

const (
	DismissCallEnded               DismissReason = 0
	DismissAnsweredOnAnotherDevice DismissReason = 1
	DismissInAnotherCall           DismissReason = 2
	DismissRejectedOnAnotherDevice DismissReason = 4
	DismissRejectedByCallee        DismissReason = 6
)

// RingType is multiway_RingType.
type RingType int32

const (
	RingGroupAudio RingType = 0
	RingPeerVideo  RingType = 1
	RingPeerAudio  RingType = 2
	RingGroupVideo RingType = 3
)

// MediaPath is multiway_MediaPath. E2EE 1:1 calls negotiate P2P: the server
// only relays SDP and ICE, media flows peer to peer (or via Meta TURN).
type MediaPath int32

const (
	MediaPathUnknown MediaPath = 0
	MediaPathSFU     MediaPath = 1
	MediaPathP2P     MediaPath = 2
)

// E2eeMode is multiway_E2eeMode (E2eeEnforcement.mode).
type E2eeMode int32

const (
	E2eeNotMandated E2eeMode = 0
	E2eeMandated    E2eeMode = 2
)

// DeviceStatus is multiway_DeviceStatus (RingResponse.deviceStatus).
type DeviceStatus int32

const (
	DeviceStatusOK            DeviceStatus = 0
	DeviceStatusNotSupported  DeviceStatus = 1
	DeviceStatusInAnotherCall DeviceStatus = 10
)

// ClientEventType is multiway_ClientEventType.
type ClientEventType int32

const ClientEventMediaConnected ClientEventType = 1

// Capability is multiway_Capability, sent in JoinRequest.deviceCapabilities.
type Capability int32

// WebDeviceCapabilities is the capability set facebook.com sent in the
// captured join (SUPPORT_NEW_PARTICIPANT_STATES, REQUIRE_FULL_SDP_IN_SMU,
// SUPPORT_SDP_RENEGOTIATION, REQUIRE_FULL_SDP_IN_SMU_OPTIMIZED,
// SUPPORT_DELTA_SMU, SUPPORT_PRECONNECT, SUPPORT_MWPP,
// SUPPORT_MWPP_DEESCALATION, SUPPORT_MULTIPLE_VIDEO_STREAMS).
var WebDeviceCapabilities = []int32{3, 6, 4, 11, 13, 7, 5, 8, 10}

// Status codes for RtcMessageHeader.responseStatusCode.
const (
	StatusOK = 200
	// SubCodeOK is the responseSubCode the server puts on successful replies.
	SubCodeOK = 9000
)
