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
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"slices"
	"strconv"
	"sync/atomic"
)

// Builders for the messages a client sends, shaped like the web client's
// (ZenonMWThrift*Translator). Header layouts observed on the wire:
//
//	request:            type, conferenceName, transactionId, retryCount,
//	                    [serverInfoData], sequenceNumber, clientSessionId,
//	                    conferenceType=ROOM, clientStack=ZENON, sender, tags={}
//	response:           the same with responseStatusCode=200, the request's
//	                    transactionId and tags; the RingResponse from the
//	                    parent window has no sender.

// randomDecimal returns a random non-negative 31-bit number as a decimal
// string, the form the web client uses for transaction and session ids.
func randomDecimal() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return strconv.FormatUint(uint64(binary.BigEndian.Uint32(b[:])&0x7fffffff), 10)
}

// NewClientSessionID returns a fresh clientSessionId (one per call window).
func NewClientSessionID() string { return randomDecimal() }

// NewTransactionID returns a fresh transactionId for a client request.
func NewTransactionID() string { return randomDecimal() }

// CallContext holds the per-call header state of one client session: who
// we are, the conference and call handle, and the sequence counter that
// requests and responses share.
type CallContext struct {
	SelfID          string
	ClientSessionID string
	ConferenceName  string
	ServerInfoData  string
	seq             atomic.Int64
}

// NewCallContext starts a client session for a call.
func NewCallContext(selfID, conferenceName, serverInfoData string) *CallContext {
	return &CallContext{
		SelfID:          selfID,
		ClientSessionID: NewClientSessionID(),
		ConferenceName:  conferenceName,
		ServerInfoData:  serverInfoData,
	}
}

func (c *CallContext) nextSeq() int64 { return c.seq.Add(1) - 1 }

// Request builds a client request with a fresh transaction id.
func (c *CallContext) Request(typ MessageType, body Body) *Message {
	return &Message{
		Header: Header{
			Type:            typ,
			ConferenceName:  c.ConferenceName,
			TransactionID:   NewTransactionID(),
			ServerInfoData:  c.ServerInfoData,
			SequenceNumber:  c.nextSeq(),
			ClientSessionID: c.ClientSessionID,
			ConferenceType:  ConferenceTypeRoom,
			ClientStack:     ClientStackZenon,
			SenderID:        c.SelfID,
			MessageTags:     []int32{},
		},
		Body: body,
	}
}

// Respond builds the acknowledgement of a server request.
func (c *CallContext) Respond(req *Message, body Body) *Message {
	return NewResponse(req, c.nextSeq(), c.SelfID, c.ClientSessionID, body)
}

// DefaultResponseBody is the response body the web client sends for a
// server-initiated request of the given message (observed in the captures
// and in the ZenonMWThrift*Translator.toThrift*Response functions).
func DefaultResponseBody(req *Message) Body {
	b := &req.Body
	switch {
	case b.RingRequest != nil:
		return Body{RingResponse: &RingResponse{DeviceStatus: DeviceStatusOK}}
	case b.ConferenceStateRequest != nil:
		return Body{ConferenceStateResponse: &ConferenceStateResponse{CurrentVersion: b.ConferenceStateRequest.Version}}
	case b.ServerMediaUpdateRequest != nil:
		return Body{ServerMediaUpdateResponse: &ServerMediaUpdateResponse{CurrentVersion: b.ServerMediaUpdateRequest.ToVersion}}
	case b.DataMessageRequest != nil:
		return Body{DataMessageResponse: &DataMessageResponse{}}
	case b.NotifyRequest != nil:
		topic, version := b.NotifyRequest.Topic, b.NotifyRequest.Version
		// A batched notify is acknowledged per contained topic; the web
		// client answered with the first topic of the batch.
		if sp := b.NotifyRequest.SyncPayload; topic == "batched_notify" && sp != nil && len(sp.StateStore) > 0 {
			topic, version = sp.StateStore[0].Topic, sp.StateStore[0].Version
		}
		return Body{NotifyResponse: &StateSyncMessage{Topic: topic, Version: version}}
	}
	// ICE_CANDIDATE, SUBSCRIPTION, DISMISS and HANGUP are acknowledged with
	// an empty body.
	return Body{}
}

// WebUserCapabilities is the userCapabilities JSON of the web client.
const WebUserCapabilities = `{"AddParticipantEnabled":false,"GROUP_COWATCH":true,"MultipleVideoStreamsAllowed":true,"MW_AV_ESCALATION":true,"canApproveCollaborationSpaceJoinRequests":true,"cowatch":true,"screen_sharing":false,"sctpSecondPc":false}`

// JoiningContextTopic is the app message topic carrying the joining context.
const JoiningContextTopic = "joining_context"

// JoiningContext is the JSON the web client puts in its JOIN.
type JoiningContext struct {
	CallTrigger     *string `json:"call_trigger"`
	CallablePostID  *string `json:"callable_post_id"`
	CallingTags     int     `json:"calling_tags"`
	GroupThreadID   *string `json:"group_thread_id"`
	IGThreadID      *string `json:"ig_thread_id"`
	LinkURL         *string `json:"link_url"`
	LiveBroadcastID *string `json:"live_broadcast_id"`
	MeetingID       *string `json:"meeting_id"`
	PeerID          string  `json:"peer_id"`
	ServerInfoData  *string `json:"server_info_data"`
}

// JoinParams are the variable parts of a 1:1 audio JOIN.
type JoinParams struct {
	// Offer is set by a caller, Answer by a callee answering a RING offer.
	Offer, Answer string
	// PeerID is the other user of the 1:1 thread.
	PeerID string
	// UsersToCall is set by a caller (the peer); empty for a callee.
	UsersToCall []string
	// AudioTrackID is the msid track id of our audio track in the SDP.
	AudioTrackID string
	// E2eeState is our marshalled E2eeClientState (optional).
	E2eeState []byte
	// E2eeMandated is true for end-to-end encrypted threads.
	E2eeMandated bool
}

// coplayInitialState is the web client's initial "coplay" state (an empty
// compact struct with i32 field 1 = 0).
var coplayInitialState = []byte{0x15, 0x00}

// NewJoin builds a JOIN as the web client sends it in P2P (MWPP) mode:
// a caller puts its offer in field 1, a callee sends an empty offer struct
// and its answer in field 14 (ZenonMWThriftJoinTranslator: `offer:{}` is
// always present, `answer` is set when the local SDP is an answer, and
// SUPPORT_MWPP is advertised). clientMediaMode is P2P.
func (c *CallContext) NewJoin(p *JoinParams) *Message {
	var sid *string
	if c.ServerInfoData != "" {
		sid = &c.ServerInfoData
	}
	jc := JoiningContext{CallingTags: 2, PeerID: p.PeerID, ServerInfoData: sid}
	if !p.E2eeMandated {
		jc.CallingTags = 0
	}
	jcJSON, _ := json.Marshal(&jc)
	jr := &JoinRequest{
		Offer:              &SessionDescription{SDP: p.Offer},
		DeviceCapabilities: slices.Clone(WebDeviceCapabilities),
		UsersToCall:        slices.Clone(p.UsersToCall),
		MediaStatus:        map[string]bool{},
		UserCapabilities:   WebUserCapabilities,
		AppMessages: []DataMessage{{
			TopicDeprecated: JoiningContextTopic,
			Topic:           JoiningContextTopic,
			Data:            jcJSON,
		}},
		MediaStatusEx:   map[string]TrackInfo{},
		ClientMediaMode: int32(MediaPathP2P),
		JoinMode:        new(int32),
	}
	if jr.UsersToCall == nil {
		jr.UsersToCall = []string{}
	}
	if p.AudioTrackID != "" {
		jr.MediaStatus[p.AudioTrackID] = true
		jr.MediaStatusEx[p.AudioTrackID] = TrackInfo{Enabled: true}
	}
	if p.Answer != "" {
		jr.Answer = &SessionDescription{SDP: p.Answer}
	}
	store := StateStore{{Topic: "coplay", State: State{Version: 1, Data: coplayInitialState}}}
	if p.E2eeState != nil {
		store = append(store, TopicState{Topic: TopicE2eeState, State: State{Version: 1, Data: p.E2eeState}})
	}
	jr.SyncPayload = &SyncPayload{StateStore: store}
	if p.E2eeMandated {
		jr.E2eeEnforcement = &E2eeEnforcement{Mode: E2eeMandated}
	} else {
		jr.E2eeEnforcement = &E2eeEnforcement{Mode: E2eeNotMandated}
	}
	return c.Request(TypeJoin, Body{JoinRequest: jr})
}

// NewIceCandidates builds an ICE_CANDIDATE request (the web client sends
// one candidate per message).
func (c *CallContext) NewIceCandidates(cands ...IceCandidate) *Message {
	return c.Request(TypeIceCandidate, Body{IceCandidateRequest: &IceCandidateRequest{Candidates: cands}})
}

// NewHangup builds a HANGUP. reason is HangupHangupCall to leave a call and
// HangupIgnoreCall to decline a ring (ZenonDismissReason.IgnoreCall).
func (c *CallContext) NewHangup(reason HangupReason) *Message {
	return c.Request(TypeHangup, Body{HangupRequest: &HangupRequest{Reason: reason}})
}

// NewMediaConnected builds the CLIENT_EVENT MEDIA_CONNECTED request.
func (c *CallContext) NewMediaConnected() *Message {
	return c.Request(TypeClientEvent, Body{ClientEventRequest: &ClientEventRequest{
		Events: []ClientEvent{{Type: ClientEventMediaConnected}},
	}})
}

// NewDominantSpeakerSubscription builds the SUBSCRIPTION the web client
// sends after joining.
func (c *CallContext) NewDominantSpeakerSubscription() *Message {
	return c.Request(TypeSubscription, Body{SubscriptionRequest: &SubscriptionRequest{
		Subscriptions: []Subscription{{Type: 2}},
	}})
}

// NewRingResponse builds the parent window's reply to a RING: status 200,
// RingResponse{deviceStatus}, and no sender in the header.
func NewRingResponse(ring *Message, sessionID string, status DeviceStatus) *Message {
	return NewResponse(ring, 0, "", sessionID, Body{RingResponse: &RingResponse{DeviceStatus: status}})
}
