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
	"errors"
	"fmt"
	"maps"
	"slices"
)

// Header is RtcMessageHeader. Field comments give the Thrift field id.
type Header struct {
	Type                  MessageType       // 1
	ConferenceName        string            // 2, "ROOM:<id>"; empty in the first JOIN
	TransactionID         string            // 3, decimal string, echoed by the response
	RetryCount            int16             // 4
	ServerInfoData        string            // 5, opaque call handle (base64 thrift), see ParseServerInfoData
	ResponseStatusCode    int32             // 6, 200 on responses, 0 on requests
	Extensions            map[string]string // 7
	SequenceNumber        int64             // 8, per-direction counter
	ClientSessionID       string            // 9, random per call window
	ResponseStatusMessage string            // 10
	ResponseSubCode       int32             // 11
	CollisionKey          string            // 12
	ConferenceType        ConferenceType    // 13
	ServerSessionID       string            // 14
	RtcHandle             string            // 15
	RetryAfterMsec        int32             // 16
	ReceiverUserID        string            // 17, set by the server
	ClientStack           ClientStack       // 18, set by the client (5 = ZENON)
	ServerMsgTime         int64             // 19, ms since epoch
	SenderID              string            // 20 (RtcSender.id)
	ReceiverActorID       string            // 21 (RtcReceiver.actorId)
	ReceiverBaseID        string            // 21 (RtcReceiver.baseId)
	MessageTags           []int32           // 22 (set<MessageTag>)
	ConferenceID          int64             // 23
	ProtocolVersion       int32             // 24
	BodyCompressionVer    int64             // 25

	// present records which fields were on the wire, so re-encoding a
	// decoded header is byte-for-byte stable.
	present map[int16]bool
}

// Has reports whether the given header field id was present on the wire.
func (h *Header) Has(id int16) bool { return h.present[id] }

func (h *Header) set(id int16) {
	if h.present == nil {
		h.present = make(map[int16]bool)
	}
	h.present[id] = true
}

// IsResponse reports whether the message is a reply to an earlier request.
func (h *Header) IsResponse() bool { return h.ResponseStatusCode != 0 }

func (h *Header) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) (err error) {
		h.set(id)
		switch {
		case id == 1 && t == TypeI32:
			var v int32
			v, err = r.i32()
			h.Type = MessageType(v)
		case id == 2 && t == TypeBinary:
			h.ConferenceName, err = r.string()
		case id == 3 && t == TypeBinary:
			h.TransactionID, err = r.string()
		case id == 4 && t == TypeI16:
			h.RetryCount, err = r.i16()
		case id == 5 && t == TypeBinary:
			h.ServerInfoData, err = r.string()
		case id == 6 && t == TypeI32:
			h.ResponseStatusCode, err = r.i32()
		case id == 7 && t == TypeMap:
			h.Extensions = map[string]string{}
			err = r.stringMap(func(k string, vt Type) error {
				if vt != TypeBinary {
					return r.skip(vt)
				}
				v, err := r.string()
				h.Extensions[k] = v
				return err
			})
		case id == 8 && t == TypeI64:
			h.SequenceNumber, err = r.i64()
		case id == 9 && t == TypeBinary:
			h.ClientSessionID, err = r.string()
		case id == 10 && t == TypeBinary:
			h.ResponseStatusMessage, err = r.string()
		case id == 11 && t == TypeI32:
			h.ResponseSubCode, err = r.i32()
		case id == 12 && t == TypeBinary:
			h.CollisionKey, err = r.string()
		case id == 13 && t == TypeI32:
			var v int32
			v, err = r.i32()
			h.ConferenceType = ConferenceType(v)
		case id == 14 && t == TypeBinary:
			h.ServerSessionID, err = r.string()
		case id == 15 && t == TypeBinary:
			h.RtcHandle, err = r.string()
		case id == 16 && t == TypeI32:
			h.RetryAfterMsec, err = r.i32()
		case id == 17 && t == TypeBinary:
			h.ReceiverUserID, err = r.string()
		case id == 18 && t == TypeI32:
			var v int32
			v, err = r.i32()
			h.ClientStack = ClientStack(v)
		case id == 19 && t == TypeI64:
			h.ServerMsgTime, err = r.i64()
		case id == 20 && t == TypeStruct:
			err = r.readStruct(func(id int16, t Type) (err error) {
				if id == 1 && t == TypeBinary {
					h.SenderID, err = r.string()
					return
				}
				return r.skip(t)
			})
		case id == 21 && t == TypeStruct:
			err = r.readStruct(func(id int16, t Type) (err error) {
				switch {
				case id == 1 && t == TypeBinary:
					h.ReceiverActorID, err = r.string()
				case id == 2 && t == TypeBinary:
					h.ReceiverBaseID, err = r.string()
				default:
					err = r.skip(t)
				}
				return
			})
		case id == 22 && (t == TypeSet || t == TypeList):
			h.MessageTags, err = r.i32List()
		case id == 23 && t == TypeI64:
			h.ConferenceID, err = r.i64()
		case id == 24 && t == TypeI32:
			h.ProtocolVersion, err = r.i32()
		case id == 25 && t == TypeI64:
			h.BodyCompressionVer, err = r.i64()
		default:
			err = r.skip(t)
		}
		return
	})
}

// encode writes the header. A decoded header re-emits exactly the fields it
// had on the wire. A freshly built one writes the fields the web client
// always sends (type, conferenceName, transactionId, retryCount,
// sequenceNumber, messageTags) plus every non-zero optional field.
func (h *Header) encode(w *writer) {
	has := func(id int16, nonzero bool) bool {
		if h.present != nil {
			return h.present[id]
		}
		return nonzero
	}
	w.structBegin()
	if has(1, true) {
		w.fieldI32(1, int32(h.Type))
	}
	if has(2, true) {
		w.fieldString(2, h.ConferenceName)
	}
	if has(3, true) {
		w.fieldString(3, h.TransactionID)
	}
	if has(4, true) {
		w.fieldI16(4, h.RetryCount)
	}
	if has(5, h.ServerInfoData != "") {
		w.fieldString(5, h.ServerInfoData)
	}
	if has(6, h.ResponseStatusCode != 0) {
		w.fieldI32(6, h.ResponseStatusCode)
	}
	if has(7, len(h.Extensions) > 0) {
		w.fieldHeader(7, TypeMap)
		w.mapHeader(TypeBinary, TypeBinary, len(h.Extensions))
		for _, k := range slices.Sorted(maps.Keys(h.Extensions)) {
			w.binaryValue([]byte(k))
			w.binaryValue([]byte(h.Extensions[k]))
		}
	}
	if has(8, true) {
		w.fieldI64(8, h.SequenceNumber)
	}
	if has(9, h.ClientSessionID != "") {
		w.fieldString(9, h.ClientSessionID)
	}
	if has(10, h.ResponseStatusMessage != "") {
		w.fieldString(10, h.ResponseStatusMessage)
	}
	if has(11, h.ResponseSubCode != 0) {
		w.fieldI32(11, h.ResponseSubCode)
	}
	if has(12, h.CollisionKey != "") {
		w.fieldString(12, h.CollisionKey)
	}
	if has(13, h.ConferenceType != 0) {
		w.fieldI32(13, int32(h.ConferenceType))
	}
	if has(14, h.ServerSessionID != "") {
		w.fieldString(14, h.ServerSessionID)
	}
	if has(15, h.RtcHandle != "") {
		w.fieldString(15, h.RtcHandle)
	}
	if has(16, h.RetryAfterMsec != 0) {
		w.fieldI32(16, h.RetryAfterMsec)
	}
	if has(17, h.ReceiverUserID != "") {
		w.fieldString(17, h.ReceiverUserID)
	}
	if has(18, h.ClientStack != 0) {
		w.fieldI32(18, int32(h.ClientStack))
	}
	if has(19, h.ServerMsgTime != 0) {
		w.fieldI64(19, h.ServerMsgTime)
	}
	if has(20, h.SenderID != "") {
		w.fieldStruct(20, func() { w.fieldString(1, h.SenderID) })
	}
	if has(21, h.ReceiverActorID != "" || h.ReceiverBaseID != "") {
		w.fieldStruct(21, func() {
			w.optString(1, h.ReceiverActorID)
			w.optString(2, h.ReceiverBaseID)
		})
	}
	if has(22, true) {
		w.fieldI32List(22, TypeSet, h.MessageTags)
	}
	if has(23, h.ConferenceID != 0) {
		w.fieldI64(23, h.ConferenceID)
	}
	if has(24, h.ProtocolVersion != 0) {
		w.fieldI32(24, h.ProtocolVersion)
	}
	if has(25, h.BodyCompressionVer != 0) {
		w.fieldI64(25, h.BodyCompressionVer)
	}
	w.structEnd()
}

// Message is one multiway signalling message: an RtcMessageHeader followed
// directly by an RtcMessageBody (a union), both Thrift compact structs,
// concatenated without framing (ZenonMWThriftMessageSerializer with the
// MQTT header omitted).
type Message struct {
	Header Header
	Body   Body
}

// Body is RtcMessageBody. Exactly one variant is normally set. Responses to
// ICE_CANDIDATE and SUBSCRIPTION carry an empty body (FieldID 0).
type Body struct {
	// FieldID is the union member id that was present (0 = empty body).
	FieldID int16
	// Raw is the encoded member struct as received, including its stop byte.
	// It is used to re-encode members this package does not model.
	Raw []byte

	JoinRequest               *JoinRequest               // 1
	JoinResponse              *JoinResponse              // 2
	ServerMediaUpdateRequest  *ServerMediaUpdateRequest  // 3
	ServerMediaUpdateResponse *ServerMediaUpdateResponse // 4
	HangupRequest             *HangupRequest             // 5
	IceCandidateRequest       *IceCandidateRequest       // 6
	RingRequest               *RingRequest               // 8
	RingResponse              *RingResponse              // 9
	DismissRequest            *DismissRequest            // 10
	ConferenceStateRequest    *ConferenceStateRequest    // 11
	ConferenceStateResponse   *ConferenceStateResponse   // 12
	SubscriptionRequest       *SubscriptionRequest       // 14
	ClientMediaUpdateRequest  *ClientMediaUpdateRequest  // 15
	ClientMediaUpdateResponse *ClientMediaUpdateResponse // 16
	DataMessageRequest        *DataMessageRequest        // 17
	DataMessageResponse       *DataMessageResponse       // 19
	UpdateRequest             *StateSyncMessage          // 30
	UpdateResponse            *StateSyncMessage          // 31
	NotifyRequest             *StateSyncMessage          // 32
	NotifyResponse            *StateSyncMessage          // 33
	ConnectRequest            *ConnectMessage            // 34
	ConnectResponse           *ConnectMessage            // 35
	ClientEventRequest        *ClientEventRequest        // 36
	ClientEventResponse       *struct{}                  // 37
}

// BodyFieldNames maps RtcMessageBody member ids to their Thrift names.
var BodyFieldNames = map[int16]string{
	1: "joinRequest", 2: "joinResponse", 3: "serverMediaUpdateRequest",
	4: "serverMediaUpdateResponse", 5: "hangupRequest", 6: "iceCandidateRequest",
	8: "ringRequest", 9: "ringResponse", 10: "dismissRequest",
	11: "conferenceStateRequest", 12: "conferenceStateResponse",
	13: "addParticipantsRequest", 14: "subscriptionRequest",
	15: "clientMediaUpdateRequest", 16: "clientMediaUpdateResponse",
	17: "dataMessageRequest", 18: "removeParticipantsRequest", 19: "dataMessageResponse",
	30: "updateRequest", 31: "updateResponse", 32: "notifyRequest", 33: "notifyResponse",
	34: "connectRequest", 35: "connectResponse", 36: "clientEventRequest",
	37: "clientEventResponse", 40: "unsubscribeRequest", 41: "unsubscribeResponse",
	42: "approvalRequest", 43: "transferRequest",
}

// Name returns the Thrift name of the body member, or "" for an empty body.
func (b *Body) Name() string {
	if b.FieldID == 0 {
		return ""
	}
	if n, ok := BodyFieldNames[b.FieldID]; ok {
		return n
	}
	return fmt.Sprintf("field%d", b.FieldID)
}

func (b *Body) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) error {
		if t != TypeStruct {
			return r.skip(t)
		}
		if b.FieldID != 0 {
			return errors.New("rtcsignal: more than one body member set")
		}
		raw, err := r.rawStruct()
		if err != nil {
			return err
		}
		b.FieldID = id
		b.Raw = raw
		sub := newReader(raw)
		switch id {
		case 1:
			b.JoinRequest = &JoinRequest{}
			err = b.JoinRequest.decode(sub)
		case 2:
			b.JoinResponse = &JoinResponse{}
			err = b.JoinResponse.decode(sub)
		case 3:
			b.ServerMediaUpdateRequest = &ServerMediaUpdateRequest{}
			err = b.ServerMediaUpdateRequest.decode(sub)
		case 4:
			b.ServerMediaUpdateResponse = &ServerMediaUpdateResponse{}
			err = b.ServerMediaUpdateResponse.decode(sub)
		case 5:
			b.HangupRequest = &HangupRequest{}
			err = b.HangupRequest.decode(sub)
		case 6:
			b.IceCandidateRequest = &IceCandidateRequest{}
			err = b.IceCandidateRequest.decode(sub)
		case 8:
			b.RingRequest = &RingRequest{}
			err = b.RingRequest.decode(sub)
		case 9:
			b.RingResponse = &RingResponse{}
			err = b.RingResponse.decode(sub)
		case 10:
			b.DismissRequest = &DismissRequest{}
			err = b.DismissRequest.decode(sub)
		case 11:
			b.ConferenceStateRequest = &ConferenceStateRequest{}
			err = b.ConferenceStateRequest.decode(sub)
		case 12:
			b.ConferenceStateResponse = &ConferenceStateResponse{}
			err = b.ConferenceStateResponse.decode(sub)
		case 14:
			b.SubscriptionRequest = &SubscriptionRequest{}
			err = b.SubscriptionRequest.decode(sub)
		case 15:
			b.ClientMediaUpdateRequest = &ClientMediaUpdateRequest{}
			err = b.ClientMediaUpdateRequest.decode(sub)
		case 16:
			b.ClientMediaUpdateResponse = &ClientMediaUpdateResponse{}
			err = b.ClientMediaUpdateResponse.decode(sub)
		case 17:
			b.DataMessageRequest = &DataMessageRequest{}
			err = b.DataMessageRequest.decode(sub)
		case 19:
			b.DataMessageResponse = &DataMessageResponse{}
			err = b.DataMessageResponse.decode(sub)
		case 30, 31, 32, 33:
			m := &StateSyncMessage{}
			err = m.decode(sub, id)
			switch id {
			case 30:
				b.UpdateRequest = m
			case 31:
				b.UpdateResponse = m
			case 32:
				b.NotifyRequest = m
			case 33:
				b.NotifyResponse = m
			}
		case 34, 35:
			m := &ConnectMessage{}
			err = m.decode(sub)
			if id == 34 {
				b.ConnectRequest = m
			} else {
				b.ConnectResponse = m
			}
		case 36:
			b.ClientEventRequest = &ClientEventRequest{}
			err = b.ClientEventRequest.decode(sub)
		case 37:
			b.ClientEventResponse = &struct{}{}
		}
		if err != nil {
			return fmt.Errorf("%s: %w", b.Name(), err)
		}
		return nil
	})
}

type bodyEncoder interface{ encode(w *writer) }

// member returns the id and encoder of the typed member that is set. Typed
// members win over Raw, so re-encoding a decoded message drops fields this
// package does not model; to relay a message verbatim, leave only Raw set.
func (b *Body) member() (int16, bodyEncoder) {
	switch {
	case b.JoinRequest != nil:
		return 1, b.JoinRequest
	case b.JoinResponse != nil:
		return 2, b.JoinResponse
	case b.ServerMediaUpdateRequest != nil:
		return 3, b.ServerMediaUpdateRequest
	case b.ServerMediaUpdateResponse != nil:
		return 4, b.ServerMediaUpdateResponse
	case b.HangupRequest != nil:
		return 5, b.HangupRequest
	case b.IceCandidateRequest != nil:
		return 6, b.IceCandidateRequest
	case b.RingRequest != nil:
		return 8, b.RingRequest
	case b.RingResponse != nil:
		return 9, b.RingResponse
	case b.DismissRequest != nil:
		return 10, b.DismissRequest
	case b.ConferenceStateRequest != nil:
		return 11, b.ConferenceStateRequest
	case b.ConferenceStateResponse != nil:
		return 12, b.ConferenceStateResponse
	case b.SubscriptionRequest != nil:
		return 14, b.SubscriptionRequest
	case b.ClientMediaUpdateRequest != nil:
		return 15, b.ClientMediaUpdateRequest
	case b.ClientMediaUpdateResponse != nil:
		return 16, b.ClientMediaUpdateResponse
	case b.DataMessageRequest != nil:
		return 17, b.DataMessageRequest
	case b.DataMessageResponse != nil:
		return 19, b.DataMessageResponse
	case b.UpdateRequest != nil:
		return 30, stateSyncEncoder{b.UpdateRequest, 30}
	case b.UpdateResponse != nil:
		return 31, stateSyncEncoder{b.UpdateResponse, 31}
	case b.NotifyRequest != nil:
		return 32, stateSyncEncoder{b.NotifyRequest, 32}
	case b.NotifyResponse != nil:
		return 33, stateSyncEncoder{b.NotifyResponse, 33}
	case b.ConnectRequest != nil:
		return 34, b.ConnectRequest
	case b.ConnectResponse != nil:
		return 35, b.ConnectResponse
	case b.ClientEventRequest != nil:
		return 36, b.ClientEventRequest
	case b.ClientEventResponse != nil:
		return 37, emptyStruct{}
	}
	return 0, nil
}

type emptyStruct struct{}

func (emptyStruct) encode(w *writer) {}

func (b *Body) encode(w *writer) error {
	w.structBegin()
	if id, enc := b.member(); enc != nil {
		w.fieldStruct(id, func() { enc.encode(w) })
	} else if b.FieldID != 0 && b.Raw != nil {
		w.fieldRawStruct(b.FieldID, b.Raw)
	} else if b.FieldID != 0 {
		return fmt.Errorf("rtcsignal: encoding %s is not supported", b.Name())
	}
	w.structEnd()
	return nil
}

// Unmarshal decodes a multiway message (header followed by body).
func Unmarshal(data []byte) (*Message, error) {
	r := newReader(data)
	msg := &Message{}
	if err := msg.Header.decode(r); err != nil {
		return nil, fmt.Errorf("rtcsignal: header: %w", err)
	}
	if r.remaining() > 0 {
		if err := msg.Body.decode(r); err != nil {
			return nil, fmt.Errorf("rtcsignal: body: %w", err)
		}
	}
	if r.remaining() != 0 {
		return nil, fmt.Errorf("rtcsignal: %d trailing bytes after message", r.remaining())
	}
	return msg, nil
}

// Marshal encodes the message as header followed by body.
func (m *Message) Marshal() ([]byte, error) {
	w := &writer{}
	m.Header.encode(w)
	if err := m.Body.encode(w); err != nil {
		return nil, err
	}
	return w.bytes(), nil
}

// NewResponse builds the acknowledgement the web client sends for every
// server-initiated request: same type, conference, transaction id and server
// info, status 200, and the matching response body (possibly empty).
func NewResponse(req *Message, seq int64, sender string, sessionID string, body Body) *Message {
	return &Message{
		Header: Header{
			Type:               req.Header.Type,
			ConferenceName:     req.Header.ConferenceName,
			TransactionID:      req.Header.TransactionID,
			ServerInfoData:     req.Header.ServerInfoData,
			ResponseStatusCode: StatusOK,
			SequenceNumber:     seq,
			ClientSessionID:    sessionID,
			ConferenceType:     req.Header.ConferenceType,
			ClientStack:        ClientStackZenon,
			SenderID:           sender,
			MessageTags:        slices.Clone(req.Header.MessageTags),
		},
		Body: body,
	}
}
