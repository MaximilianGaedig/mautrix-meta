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
	"maps"
	"net"
	"slices"
)

// SessionDescription carries a full SDP. The web client never compressed it
// in the capture (sdpCompressionVersion absent).
type SessionDescription struct {
	SDP                string // 1
	CompressionVersion int64  // 3
	CompressedData     []byte // 4
}

func (s *SessionDescription) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeBinary:
			s.SDP, err = r.string()
		case id == 3 && t == TypeI64:
			s.CompressionVersion, err = r.i64()
		case id == 4 && t == TypeBinary:
			s.CompressedData, err = r.binary()
		default:
			err = r.skip(t)
		}
		return
	})
}

func (s *SessionDescription) encode(w *writer) {
	if s.SDP != "" {
		w.fieldString(1, s.SDP)
	}
	if s.CompressionVersion != 0 {
		w.fieldI64(3, s.CompressionVersion)
	}
	if s.CompressedData != nil {
		w.fieldBinary(4, s.CompressedData)
	}
}

func decodeSD(r *reader) (*SessionDescription, error) {
	sd := &SessionDescription{}
	return sd, sd.decode(r)
}

func (w *writer) optSD(id int16, sd *SessionDescription) {
	if sd != nil {
		w.fieldStruct(id, func() { sd.encode(w) })
	}
}

// IceCandidate is one trickled candidate ("candidate:..." without "a=").
type IceCandidate struct {
	Candidate     string // 1
	SDPMLineIndex int64  // 2
	SDPMid        string // 3
}

// IceCandidateRequest (body member 6) trickles candidates in both
// directions. The server's reply to a client request, and the client's reply
// to a server request, is an ICE_CANDIDATE message with an empty body.
type IceCandidateRequest struct {
	Candidates []IceCandidate // 1
}

func (m *IceCandidateRequest) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) error {
		if id != 1 || t != TypeList {
			return r.skip(t)
		}
		return r.structList(func() error {
			var c IceCandidate
			err := r.readStruct(func(id int16, t Type) (err error) {
				switch {
				case id == 1 && t == TypeBinary:
					c.Candidate, err = r.string()
				case id == 2 && t == TypeI64:
					c.SDPMLineIndex, err = r.i64()
				case id == 3 && t == TypeBinary:
					c.SDPMid, err = r.string()
				default:
					err = r.skip(t)
				}
				return
			})
			m.Candidates = append(m.Candidates, c)
			return err
		})
	})
}

func (m *IceCandidateRequest) encode(w *writer) {
	w.fieldHeader(1, TypeList)
	w.listHeader(TypeStruct, len(m.Candidates))
	for _, c := range m.Candidates {
		w.structBegin()
		w.fieldString(1, c.Candidate)
		w.fieldI64(2, c.SDPMLineIndex)
		w.fieldString(3, c.SDPMid)
		w.structEnd()
	}
}

// DataMessage is a multiway app message (DataMessage with a DataHeader and a
// GenericDataMessage body). Topics seen: "joining_context" (JSON, in JOIN),
// "collision_context_payload" (JSON, in CONFERENCE_STATE), and in the JS
// "E2eeKey" for group-call (SFU) sender keys.
type DataMessage struct {
	Sender             string   // header.1
	TopicDeprecated    string   // header.2
	Recipients         []string // header.3
	ServiceSender      int32    // header.4
	ServiceRecipients  []int32  // header.5 (set<i32>)
	ShouldSendToAll    bool     // header.6
	SenderE2eeID       string   // header.7
	Topic              string   // body.genericMessage.1
	Data               []byte   // body.genericMessage.2
	E2eEncryptedData   []byte   // body.genericMessage.3
	HasOtherBodyMember bool     // body had a non-generic member (not modelled)

	// hasServiceSender/hasServiceRecipients record wire presence of header
	// fields 4 and 5 (the web client sends an empty set 5 on E2eeKey
	// messages), so a decoded message re-encodes byte for byte.
	hasServiceSender     bool
	hasServiceRecipients bool
}

func (d *DataMessage) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) error {
		switch {
		case id == 1 && t == TypeStruct:
			return r.readStruct(func(id int16, t Type) (err error) {
				switch {
				case id == 1 && t == TypeBinary:
					d.Sender, err = r.string()
				case id == 2 && t == TypeBinary:
					d.TopicDeprecated, err = r.string()
				case id == 3 && (t == TypeSet || t == TypeList):
					d.Recipients, err = r.stringList()
				case id == 4 && t == TypeI32:
					d.hasServiceSender = true
					d.ServiceSender, err = r.i32()
				case id == 5 && (t == TypeSet || t == TypeList):
					d.hasServiceRecipients = true
					d.ServiceRecipients, err = r.i32List()
				case id == 6 && (t == TypeTrue || t == TypeFalse):
					d.ShouldSendToAll = t == TypeTrue
				case id == 7 && t == TypeBinary:
					d.SenderE2eeID, err = r.string()
				default:
					err = r.skip(t)
				}
				return
			})
		case id == 3 && t == TypeStruct:
			return r.readStruct(func(id int16, t Type) error {
				if id != 1 || t != TypeStruct {
					d.HasOtherBodyMember = true
					return r.skip(t)
				}
				return r.readStruct(func(id int16, t Type) (err error) {
					switch {
					case id == 1 && t == TypeBinary:
						d.Topic, err = r.string()
					case id == 2 && t == TypeBinary:
						d.Data, err = r.binary()
					case id == 3 && t == TypeBinary:
						d.E2eEncryptedData, err = r.binary()
					default:
						err = r.skip(t)
					}
					return
				})
			})
		}
		return r.skip(t)
	})
}

func (d *DataMessage) encode(w *writer) {
	w.fieldStruct(1, func() {
		w.optString(1, d.Sender)
		if d.TopicDeprecated != "" || d.Sender != "" {
			w.fieldString(2, d.TopicDeprecated)
		}
		if len(d.Recipients) > 0 {
			w.fieldStringList(3, TypeSet, d.Recipients)
		}
		if d.hasServiceSender || d.ServiceSender != 0 {
			w.fieldI32(4, d.ServiceSender)
		}
		if d.hasServiceRecipients || len(d.ServiceRecipients) > 0 {
			w.fieldI32List(5, TypeSet, d.ServiceRecipients)
		}
		if d.ShouldSendToAll {
			w.fieldBool(6, true)
		}
		w.optString(7, d.SenderE2eeID)
	})
	w.fieldStruct(3, func() {
		w.fieldStruct(1, func() {
			w.fieldString(1, d.Topic)
			if d.Data != nil {
				w.fieldBinary(2, d.Data)
			}
			if d.E2eEncryptedData != nil {
				w.fieldBinary(3, d.E2eEncryptedData)
			}
		})
	})
}

func decodeDataMessages(r *reader) ([]DataMessage, error) {
	var out []DataMessage
	err := r.structList(func() error {
		var d DataMessage
		err := d.decode(r)
		out = append(out, d)
		return err
	})
	return out, err
}

func (w *writer) dataMessages(id int16, msgs []DataMessage) {
	w.fieldHeader(id, TypeList)
	w.listHeader(TypeStruct, len(msgs))
	for i := range msgs {
		w.structBegin()
		msgs[i].encode(w)
		w.structEnd()
	}
}

// ClientTrackInfo.label values seen in the server's media status.
const (
	TrackLabelAudio int32 = 0
	TrackLabelVideo int32 = 1
)

// TrackInfo is ClientTrackInfo (ClientMediaStatus.tracks value).
type TrackInfo struct {
	Enabled                bool   // 1
	PausedUplink           int32  // 2
	PausedDownlink         int32  // 3
	Owner                  string // 4
	Label                  int32  // 5
	CustomVideoContentType int32  // 6
	Name                   string // 7
	CustomAudioContentType int32  // 8
	NodeID                 int64  // 9

	// present is a bitmask (1<<id) of the fields that were on the wire, so
	// a decoded status re-encodes exactly; zero for a freshly built one.
	present uint16
}

func (ti *TrackInfo) has(id int16, nonzero bool) bool {
	if ti.present != 0 {
		return ti.present&(1<<id) != 0
	}
	return nonzero
}

func decodeMediaStatus(r *reader) (map[string]TrackInfo, error) {
	out := map[string]TrackInfo{}
	err := r.readStruct(func(id int16, t Type) error {
		if id != 1 || t != TypeMap {
			return r.skip(t)
		}
		return r.stringMap(func(key string, vt Type) error {
			if vt != TypeStruct {
				return r.skip(vt)
			}
			var ti TrackInfo
			err := r.readStruct(func(id int16, t Type) (err error) {
				if id > 0 && id < 16 {
					ti.present |= 1 << id
				}
				switch {
				case id == 1 && (t == TypeTrue || t == TypeFalse):
					ti.Enabled = t == TypeTrue
				case id == 2 && t == TypeI32:
					ti.PausedUplink, err = r.i32()
				case id == 3 && t == TypeI32:
					ti.PausedDownlink, err = r.i32()
				case id == 4 && t == TypeBinary:
					ti.Owner, err = r.string()
				case id == 5 && t == TypeI32:
					ti.Label, err = r.i32()
				case id == 6 && t == TypeI32:
					ti.CustomVideoContentType, err = r.i32()
				case id == 7 && t == TypeBinary:
					ti.Name, err = r.string()
				case id == 8 && t == TypeI32:
					ti.CustomAudioContentType, err = r.i32()
				case id == 9 && t == TypeI64:
					ti.NodeID, err = r.i64()
				default:
					err = r.skip(t)
				}
				return
			})
			out[key] = ti
			return err
		})
	})
	return out, err
}

func (w *writer) mediaStatus(id int16, tracks map[string]TrackInfo) {
	w.fieldStruct(id, func() {
		w.fieldHeader(1, TypeMap)
		w.mapHeader(TypeBinary, TypeStruct, len(tracks))
		for _, k := range slices.Sorted(maps.Keys(tracks)) {
			ti := tracks[k]
			w.binaryValue([]byte(k))
			w.structBegin()
			if ti.has(1, true) {
				w.fieldBool(1, ti.Enabled)
			}
			if ti.has(2, ti.PausedUplink != 0 || ti.Owner != "") {
				w.fieldI32(2, ti.PausedUplink)
			}
			if ti.has(3, ti.PausedUplink != 0 || ti.Owner != "") {
				w.fieldI32(3, ti.PausedDownlink)
			}
			if ti.has(4, ti.Owner != "") {
				w.fieldString(4, ti.Owner)
			}
			if ti.has(5, true) {
				w.fieldI32(5, ti.Label)
			}
			if ti.has(6, true) {
				w.fieldI32(6, ti.CustomVideoContentType)
			}
			if ti.has(7, ti.Name != "") {
				w.fieldString(7, ti.Name)
			}
			if ti.has(8, true) {
				w.fieldI32(8, ti.CustomAudioContentType)
			}
			if ti.has(9, ti.NodeID != 0) {
				w.fieldI64(9, ti.NodeID)
			}
			w.structEnd()
		}
	})
}

// State is a state-sync topic value. Topics seen: "E2eeState" (a Thrift
// E2eeClientState from the client, E2eeServerState from the server),
// "coplay", "config_engine".
type State struct {
	Version int32  // 1
	Data    []byte // 2
}

// TopicE2eeState is the state-sync topic that carries the E2EE negotiation.
const TopicE2eeState = "E2eeState"

func decodeState(r *reader) (State, error) {
	var s State
	err := r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeI32:
			s.Version, err = r.i32()
		case id == 2 && t == TypeBinary:
			s.Data, err = r.binary()
		default:
			err = r.skip(t)
		}
		return
	})
	return s, err
}

// TopicState is one entry of a state store.
type TopicState struct {
	Topic string
	State
}

// StateStore is a map<string, State> that keeps wire order (the web client
// emits JS insertion order, e.g. "coplay" before "E2eeState").
type StateStore []TopicState

// Get returns the state of a topic.
func (ss StateStore) Get(topic string) (State, bool) {
	for _, e := range ss {
		if e.Topic == topic {
			return e.State, true
		}
	}
	return State{}, false
}

func decodeStateStore(r *reader) (StateStore, error) {
	out := StateStore{}
	err := r.stringMap(func(key string, vt Type) error {
		if vt != TypeStruct {
			return r.skip(vt)
		}
		s, err := decodeState(r)
		out = append(out, TopicState{Topic: key, State: s})
		return err
	})
	return out, err
}

func (w *writer) stateStoreValue(store StateStore) {
	w.mapHeader(TypeBinary, TypeStruct, len(store))
	for _, e := range store {
		w.binaryValue([]byte(e.Topic))
		w.structBegin()
		w.fieldI32(1, e.Version)
		w.fieldBinary(2, e.Data)
		w.structEnd()
	}
}

// SyncPayload wraps the state-sync stores (stateStoreV2 is keyed by numeric
// topic id and was always empty in the capture).
type SyncPayload struct {
	StateStore StateStore // 1
}

func decodeSyncPayload(r *reader) (*SyncPayload, error) {
	sp := &SyncPayload{}
	err := r.readStruct(func(id int16, t Type) (err error) {
		if id == 1 && t == TypeMap {
			sp.StateStore, err = decodeStateStore(r)
			return
		}
		return r.skip(t)
	})
	return sp, err
}

func (w *writer) syncPayload(id int16, sp *SyncPayload) {
	w.fieldStruct(id, func() {
		w.fieldHeader(1, TypeMap)
		w.stateStoreValue(sp.StateStore)
		w.fieldHeader(4, TypeMap)
		w.mapHeader(0, 0, 0)
	})
}

// E2eeEnforcement is sent by the client in JOIN (mode 2 = E2EE_MANDATED for
// an end-to-end encrypted thread).
type E2eeEnforcement struct {
	Mode                   E2eeMode // 1
	PreventSFUMode         bool     // 2
	InfraMandatedExpStatus int32    // 3
}

func decodeEnforcement(r *reader) (*E2eeEnforcement, error) {
	e := &E2eeEnforcement{}
	err := r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeI32:
			var v int32
			v, err = r.i32()
			e.Mode = E2eeMode(v)
		case id == 2 && (t == TypeTrue || t == TypeFalse):
			e.PreventSFUMode = t == TypeTrue
		case id == 3 && t == TypeI32:
			e.InfraMandatedExpStatus, err = r.i32()
		default:
			err = r.skip(t)
		}
		return
	})
	return e, err
}

func (w *writer) enforcement(id int16, e *E2eeEnforcement) {
	w.fieldStruct(id, func() {
		w.fieldI32(1, int32(e.Mode))
		w.fieldBool(2, e.PreventSFUMode)
		w.fieldI32(3, e.InfraMandatedExpStatus)
	})
}

// TurnInfo is one TURN relay offered via RelayInfo. ipv4/ipv6 are Thrift
// strings holding the raw 4/16 address bytes (observed in a RingRequest);
// they are converted to text here. Observed ports: udp 40003, sslTcp 8080,
// tls 443, matching /videocall/turndiscovery/.
type TurnInfo struct {
	IPv4, IPv6                 string // 1, 2
	UDPPort                    int32  // 3
	TCPPort                    int32  // 4
	SSLTCPPort                 int32  // 5
	TLSPort                    int32  // 7
	TurnUsername, TurnPassword string // 9, 10 (per-relay credentials, if any)
}

// addrString converts a raw 4- or 16-byte address to text; anything else is
// taken to already be text.
func addrString(b []byte) string {
	if len(b) == net.IPv4len || len(b) == net.IPv6len {
		return net.IP(b).String()
	}
	return string(b)
}

// RelayInfo is the TURN allocation the server may push in JoinResponse,
// ServerMediaUpdate and RingRequest. It was absent in the captured web call
// (the page fetched TURN servers from /videocall/turndiscovery/ instead).
type RelayInfo struct {
	Turns        []TurnInfo // 1
	NumEdgerays  int        // 2 (not modelled)
	TurnUsername string     // 3
	TurnPassword string     // 4
}

func decodeRelayInfo(r *reader) (*RelayInfo, error) {
	ri := &RelayInfo{}
	err := r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeList:
			err = r.structList(func() error {
				var ti TurnInfo
				err := r.readStruct(func(id int16, t Type) (err error) {
					switch {
					case id == 1 && t == TypeBinary:
						var b []byte
						b, err = r.binary()
						ti.IPv4 = addrString(b)
					case id == 2 && t == TypeBinary:
						var b []byte
						b, err = r.binary()
						ti.IPv6 = addrString(b)
					case id == 3 && t == TypeI32:
						ti.UDPPort, err = r.i32()
					case id == 4 && t == TypeI32:
						ti.TCPPort, err = r.i32()
					case id == 5 && t == TypeI32:
						ti.SSLTCPPort, err = r.i32()
					case id == 7 && t == TypeI32:
						ti.TLSPort, err = r.i32()
					case id == 9 && t == TypeBinary:
						ti.TurnUsername, err = r.string()
					case id == 10 && t == TypeBinary:
						ti.TurnPassword, err = r.string()
					default:
						err = r.skip(t)
					}
					return
				})
				ri.Turns = append(ri.Turns, ti)
				return err
			})
		case id == 2 && t == TypeList:
			var n int
			var et Type
			if et, n, err = r.listHeader(); err == nil {
				ri.NumEdgerays = n
				for i := 0; i < n && err == nil; i++ {
					err = r.skipElem(et)
				}
			}
		case id == 3 && t == TypeBinary:
			ri.TurnUsername, err = r.string()
		case id == 4 && t == TypeBinary:
			ri.TurnPassword, err = r.string()
		default:
			err = r.skip(t)
		}
		return
	})
	return ri, err
}

// JoinRequest (body member 1) is the first message of every call: the caller
// sends it with its SDP offer and usersToCall, a callee sends it to accept a
// ring (with the answer). E2EE parameters travel in syncPayload "E2eeState".
type JoinRequest struct {
	Offer                *SessionDescription  // 1
	DeviceCapabilities   []int32              // 2 (set<Capability>)
	UsersToCall          []string             // 3
	MediaStatus          map[string]bool      // 4 (track id -> enabled)
	UserCapabilities     string               // 5 JSON
	SupportedExperiments string               // 6
	AppMessages          []DataMessage        // 9
	ConferenceType       int32                // 12
	MediaStatusEx        map[string]TrackInfo // 13
	Answer               *SessionDescription  // 14
	SyncPayload          *SyncPayload         // 15
	E2eeEnforcement      *E2eeEnforcement     // 17
	ClientMediaMode      int32                // 19
	JoinMode             *int32               // 20 (EndpointSettings.joinMode)
}

func (m *JoinRequest) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeStruct:
			m.Offer, err = decodeSD(r)
		case id == 2 && (t == TypeSet || t == TypeList):
			m.DeviceCapabilities, err = r.i32List()
		case id == 3 && (t == TypeSet || t == TypeList):
			m.UsersToCall, err = r.stringList()
		case id == 4 && t == TypeMap:
			m.MediaStatus = map[string]bool{}
			err = r.stringMap(func(k string, vt Type) error {
				v, err := r.containerBool()
				m.MediaStatus[k] = v
				return err
			})
		case id == 5 && t == TypeBinary:
			m.UserCapabilities, err = r.string()
		case id == 6 && t == TypeBinary:
			m.SupportedExperiments, err = r.string()
		case id == 9 && t == TypeList:
			m.AppMessages, err = decodeDataMessages(r)
		case id == 12 && t == TypeI32:
			m.ConferenceType, err = r.i32()
		case id == 13 && t == TypeStruct:
			m.MediaStatusEx, err = decodeMediaStatus(r)
		case id == 14 && t == TypeStruct:
			m.Answer, err = decodeSD(r)
		case id == 15 && t == TypeStruct:
			m.SyncPayload, err = decodeSyncPayload(r)
		case id == 17 && t == TypeStruct:
			m.E2eeEnforcement, err = decodeEnforcement(r)
		case id == 19 && t == TypeI32:
			m.ClientMediaMode, err = r.i32()
		case id == 20 && t == TypeStruct:
			err = r.readStruct(func(id int16, t Type) error {
				if id == 1 && t == TypeI32 {
					v, err := r.i32()
					m.JoinMode = &v
					return err
				}
				return r.skip(t)
			})
		default:
			err = r.skip(t)
		}
		return
	})
}

func (m *JoinRequest) encode(w *writer) {
	w.optSD(1, m.Offer)
	w.fieldI32List(2, TypeSet, m.DeviceCapabilities)
	w.fieldStringList(3, TypeSet, m.UsersToCall)
	w.fieldHeader(4, TypeMap)
	w.mapHeader(TypeBinary, TypeTrue, len(m.MediaStatus))
	for _, k := range slices.Sorted(maps.Keys(m.MediaStatus)) {
		w.binaryValue([]byte(k))
		if m.MediaStatus[k] {
			w.b = append(w.b, byte(TypeTrue))
		} else {
			w.b = append(w.b, byte(TypeFalse))
		}
	}
	w.optString(5, m.UserCapabilities)
	w.optString(6, m.SupportedExperiments)
	if len(m.AppMessages) > 0 {
		w.dataMessages(9, m.AppMessages)
	}
	if m.ConferenceType != 0 {
		w.fieldI32(12, m.ConferenceType)
	}
	if m.MediaStatusEx != nil {
		w.mediaStatus(13, m.MediaStatusEx)
	}
	w.optSD(14, m.Answer)
	if m.SyncPayload != nil {
		w.syncPayload(15, m.SyncPayload)
	}
	if m.E2eeEnforcement != nil {
		w.enforcement(17, m.E2eeEnforcement)
	}
	if m.ClientMediaMode != 0 {
		w.fieldI32(19, m.ClientMediaMode)
	}
	if m.JoinMode != nil {
		w.fieldStruct(20, func() { w.fieldI32(1, *m.JoinMode) })
	}
}

// JoinResponse (body member 2). For a P2P caller the answer is empty here;
// the callee's answer arrives later in a SERVER_MEDIA_UPDATE.
type JoinResponse struct {
	Answer                      *SessionDescription // 1
	Initiator                   string              // 3
	NegotiatedExperiments       string              // 4
	AppMessages                 []DataMessage       // 7
	StateStore                  StateStore          // 8
	SDPOriginLocalID            string              // 9
	IsPendingApproval           bool                // 10
	RenegotiationOffer          *SessionDescription // 11
	MultipleVideoStreamsAllowed bool                // 12
	MediaPath                   MediaPath           // 13
	GroupsOfUsers               []GroupOfUsers      // 14
	ScreenShareStreamAllowed    bool                // 15
	RelayInfo                   *RelayInfo          // 16
	SelfSCTPNodeID              int64               // 17
}

func (m *JoinResponse) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) (err error) {
		isBool := t == TypeTrue || t == TypeFalse
		switch {
		case id == 1 && t == TypeStruct:
			m.Answer, err = decodeSD(r)
		case id == 3 && t == TypeBinary:
			m.Initiator, err = r.string()
		case id == 4 && t == TypeBinary:
			m.NegotiatedExperiments, err = r.string()
		case id == 7 && t == TypeList:
			m.AppMessages, err = decodeDataMessages(r)
		case id == 8 && t == TypeMap:
			m.StateStore, err = decodeStateStore(r)
		case id == 9 && t == TypeBinary:
			m.SDPOriginLocalID, err = r.string()
		case id == 10 && isBool:
			m.IsPendingApproval = t == TypeTrue
		case id == 11 && t == TypeStruct:
			m.RenegotiationOffer, err = decodeSD(r)
		case id == 12 && isBool:
			m.MultipleVideoStreamsAllowed = t == TypeTrue
		case id == 13 && t == TypeI32:
			var v int32
			v, err = r.i32()
			m.MediaPath = MediaPath(v)
		case id == 14 && t == TypeList:
			m.GroupsOfUsers, err = decodeGroupsOfUsers(r)
		case id == 15 && isBool:
			m.ScreenShareStreamAllowed = t == TypeTrue
		case id == 16 && t == TypeStruct:
			m.RelayInfo, err = decodeRelayInfo(r)
		case id == 17 && t == TypeI64:
			m.SelfSCTPNodeID, err = r.i64()
		default:
			err = r.skip(t)
		}
		return
	})
}

func (m *JoinResponse) encode(w *writer) {
	w.optSD(1, m.Answer)
	w.optString(3, m.Initiator)
	if m.StateStore != nil {
		w.fieldHeader(8, TypeMap)
		w.stateStoreValue(m.StateStore)
	}
	w.optString(9, m.SDPOriginLocalID)
	w.fieldBool(10, m.IsPendingApproval)
	w.optSD(11, m.RenegotiationOffer)
	w.fieldBool(12, m.MultipleVideoStreamsAllowed)
	w.fieldI32(13, int32(m.MediaPath))
	if len(m.GroupsOfUsers) > 0 {
		w.groupsOfUsers(14, m.GroupsOfUsers)
	}
	w.fieldBool(15, m.ScreenShareStreamAllowed)
	if m.SelfSCTPNodeID != 0 {
		w.fieldI64(17, m.SelfSCTPNodeID)
	}
}

// ServerMediaUpdateRequest (body member 3) is how the server delivers remote
// SDP: the P2P callee's answer to the caller (tagged
// INITIAL_ANSWER_TO_P2P_CALLER), renegotiation offers, and SFU media lists.
type ServerMediaUpdateRequest struct {
	FromVersion                 int64                     // 1
	ToVersion                   int64                     // 2
	Offer                       *SessionDescription       // 4
	Answer                      *SessionDescription       // 6
	MediaStatus                 map[string]TrackInfo      // 7
	RenegotiationRequested      bool                      // 8
	PrAnswer                    *SessionDescription       // 9
	StateStore                  StateStore                // 10
	SDPOriginLocalID            string                    // 11 (user id of the SDP's author)
	MultipleVideoStreamsAllowed bool                      // 13
	RenegotiationOffer          *SessionDescription       // 14
	MediaPath                   MediaPath                 // 15
	Update                      *SessionDescriptionUpdate // 16 (delta SDP, SFU with SUPPORT_DELTA_SMU)
	ScreenShareStreamAllowed    bool                      // 17
	RelayInfo                   *RelayInfo                // 20
}

func (m *ServerMediaUpdateRequest) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) (err error) {
		isBool := t == TypeTrue || t == TypeFalse
		switch {
		case id == 1 && t == TypeI64:
			m.FromVersion, err = r.i64()
		case id == 2 && t == TypeI64:
			m.ToVersion, err = r.i64()
		case id == 4 && t == TypeStruct:
			m.Offer, err = decodeSD(r)
		case id == 6 && t == TypeStruct:
			m.Answer, err = decodeSD(r)
		case id == 7 && t == TypeStruct:
			m.MediaStatus, err = decodeMediaStatus(r)
		case id == 8 && isBool:
			m.RenegotiationRequested = t == TypeTrue
		case id == 9 && t == TypeStruct:
			m.PrAnswer, err = decodeSD(r)
		case id == 10 && t == TypeMap:
			m.StateStore, err = decodeStateStore(r)
		case id == 11 && t == TypeBinary:
			m.SDPOriginLocalID, err = r.string()
		case id == 13 && isBool:
			m.MultipleVideoStreamsAllowed = t == TypeTrue
		case id == 14 && t == TypeStruct:
			m.RenegotiationOffer, err = decodeSD(r)
		case id == 15 && t == TypeI32:
			var v int32
			v, err = r.i32()
			m.MediaPath = MediaPath(v)
		case id == 16 && t == TypeStruct:
			m.Update, err = decodeSessionDescriptionUpdate(r)
		case id == 17 && isBool:
			m.ScreenShareStreamAllowed = t == TypeTrue
		case id == 20 && t == TypeStruct:
			m.RelayInfo, err = decodeRelayInfo(r)
		default:
			err = r.skip(t)
		}
		return
	})
}

// RemoteSDP returns whichever SDP the update carries and its role.
func (m *ServerMediaUpdateRequest) RemoteSDP() (sdpType string, sd *SessionDescription) {
	switch {
	case m.Answer != nil:
		return "answer", m.Answer
	case m.Offer != nil:
		return "offer", m.Offer
	case m.RenegotiationOffer != nil:
		return "offer", m.RenegotiationOffer
	case m.PrAnswer != nil:
		return "pranswer", m.PrAnswer
	}
	return "", nil
}

// ServerMediaUpdateResponse (body member 4) acknowledges an update with the
// version the client is now at.
type ServerMediaUpdateResponse struct {
	CurrentVersion int64                // 1
	Answer         *SessionDescription  // 2
	MediaStatus    map[string]TrackInfo // 3 (the client's own tracks)
}

func (m *ServerMediaUpdateResponse) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeI64:
			m.CurrentVersion, err = r.i64()
		case id == 2 && t == TypeStruct:
			m.Answer, err = decodeSD(r)
		case id == 3 && t == TypeStruct:
			m.MediaStatus, err = decodeMediaStatus(r)
		default:
			err = r.skip(t)
		}
		return
	})
}

func (m *ServerMediaUpdateResponse) encode(w *writer) {
	w.fieldI64(1, m.CurrentVersion)
	w.optSD(2, m.Answer)
	if m.MediaStatus != nil {
		w.mediaStatus(3, m.MediaStatus)
	}
}

// HangupRequest (body member 5) leaves the call.
type HangupRequest struct {
	Reason         HangupReason // 1
	DetailedReason string       // 2
}

func (m *HangupRequest) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeI32:
			var v int32
			v, err = r.i32()
			m.Reason = HangupReason(v)
		case id == 2 && t == TypeBinary:
			m.DetailedReason, err = r.string()
		default:
			err = r.skip(t)
		}
		return
	})
}

func (m *HangupRequest) encode(w *writer) {
	w.fieldI32(1, int32(m.Reason))
	w.fieldString(2, m.DetailedReason)
}

// RingRequest (body member 8) is the incoming-call notification a callee
// receives (on the parent window's rpsignaling stream or the /t_rtc_multi
// MQTT topic). For P2P calls the caller's SDP offer is inside.
type RingRequest struct {
	Caller                string               // 1
	OtherParticipants     []string             // 2
	RingType              RingType             // 4
	OfferedExperiments    string               // 5
	IsScheduledCall       bool                 // 6
	AppMessages           []DataMessage        // 8
	Offer                 *SessionDescription  // 10
	MediaStatusEx         map[string]TrackInfo // 11 (the caller's tracks)
	IsPreconnectSupported bool                 // 12
	SDPOriginLocalID      string               // 13
	UnifiedOffer          *SessionDescription  // 14
	MediaPath             MediaPath            // 15
	E2eeEnforcement       *E2eeEnforcement     // 16
	IsLegacyCall          bool                 // 17
	IsTransferCall        bool                 // 18
	RelayInfo             *RelayInfo           // 20
	CallerClientSessionID string               // 23
	ThreadGroupID         string               // 24.1
	ThreadPeerID          string               // 24.2
	LinkURL               string               // 25
}

func (m *RingRequest) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) (err error) {
		isBool := t == TypeTrue || t == TypeFalse
		switch {
		case id == 1 && t == TypeBinary:
			m.Caller, err = r.string()
		case id == 2 && (t == TypeSet || t == TypeList):
			m.OtherParticipants, err = r.stringList()
		case id == 4 && t == TypeI32:
			var v int32
			v, err = r.i32()
			m.RingType = RingType(v)
		case id == 5 && t == TypeBinary:
			m.OfferedExperiments, err = r.string()
		case id == 6 && isBool:
			m.IsScheduledCall = t == TypeTrue
		case id == 8 && t == TypeList:
			m.AppMessages, err = decodeDataMessages(r)
		case id == 10 && t == TypeStruct:
			m.Offer, err = decodeSD(r)
		case id == 11 && t == TypeStruct:
			m.MediaStatusEx, err = decodeMediaStatus(r)
		case id == 12 && isBool:
			m.IsPreconnectSupported = t == TypeTrue
		case id == 13 && t == TypeBinary:
			m.SDPOriginLocalID, err = r.string()
		case id == 14 && t == TypeStruct:
			m.UnifiedOffer, err = decodeSD(r)
		case id == 15 && t == TypeI32:
			var v int32
			v, err = r.i32()
			m.MediaPath = MediaPath(v)
		case id == 16 && t == TypeStruct:
			m.E2eeEnforcement, err = decodeEnforcement(r)
		case id == 17 && isBool:
			m.IsLegacyCall = t == TypeTrue
		case id == 18 && isBool:
			m.IsTransferCall = t == TypeTrue
		case id == 20 && t == TypeStruct:
			m.RelayInfo, err = decodeRelayInfo(r)
		case id == 23 && t == TypeBinary:
			m.CallerClientSessionID, err = r.string()
		case id == 24 && t == TypeStruct:
			err = r.readStruct(func(id int16, t Type) (err error) {
				switch {
				case id == 1 && t == TypeBinary:
					m.ThreadGroupID, err = r.string()
				case id == 2 && t == TypeBinary:
					m.ThreadPeerID, err = r.string()
				default:
					err = r.skip(t)
				}
				return
			})
		case id == 25 && t == TypeBinary:
			m.LinkURL, err = r.string()
		default:
			err = r.skip(t)
		}
		return
	})
}

// RingResponse (body member 9) is the callee device's reply to a ring.
type RingResponse struct {
	DeviceStatus DeviceStatus // 2
}

func (m *RingResponse) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) error {
		if id == 2 && t == TypeI32 {
			v, err := r.i32()
			m.DeviceStatus = DeviceStatus(v)
			return err
		}
		return r.skip(t)
	})
}

func (m *RingResponse) encode(w *writer) { w.fieldI32(2, int32(m.DeviceStatus)) }

// DismissRequest (body member 10) stops ringing (answered/rejected elsewhere,
// caller gave up).
type DismissRequest struct {
	Reason                     DismissReason // 1
	DetailedReason             string        // 2
	CallabilityResultErrorCode int64         // 3
}

func (m *DismissRequest) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeI32:
			var v int32
			v, err = r.i32()
			m.Reason = DismissReason(v)
		case id == 2 && t == TypeBinary:
			m.DetailedReason, err = r.string()
		case id == 3 && t == TypeI64:
			m.CallabilityResultErrorCode, err = r.i64()
		default:
			err = r.skip(t)
		}
		return
	})
}

func (m *DismissRequest) encode(w *writer) {
	w.fieldI32(1, int32(m.Reason))
	w.fieldString(2, m.DetailedReason)
}

// ParticipantState is a ConferenceStateRequest.participantStates value.
type ParticipantState struct {
	State            ParticipantCallState // 1
	UserCapabilities string               // 2 JSON
	SCTPNodeID       int64                // 3
}

// ConferenceStateRequest (body member 11) is pushed by the server whenever a
// participant's state changes (CONTACTING -> RINGING -> CONNECTED, ...).
type ConferenceStateRequest struct {
	Version           int64                       // 1
	ParticipantStates map[string]ParticipantState // 2 (user id -> state)
	AppMessages       []DataMessage               // 4
}

func (m *ConferenceStateRequest) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeI64:
			m.Version, err = r.i64()
		case id == 2 && t == TypeMap:
			m.ParticipantStates = map[string]ParticipantState{}
			err = r.stringMap(func(k string, vt Type) error {
				if vt != TypeStruct {
					return r.skip(vt)
				}
				var ps ParticipantState
				err := r.readStruct(func(id int16, t Type) (err error) {
					switch {
					case id == 1 && t == TypeI32:
						var v int32
						v, err = r.i32()
						ps.State = ParticipantCallState(v)
					case id == 2 && t == TypeBinary:
						ps.UserCapabilities, err = r.string()
					case id == 3 && t == TypeI64:
						ps.SCTPNodeID, err = r.i64()
					default:
						err = r.skip(t)
					}
					return
				})
				m.ParticipantStates[k] = ps
				return err
			})
		case id == 4 && t == TypeList:
			m.AppMessages, err = decodeDataMessages(r)
		default:
			err = r.skip(t)
		}
		return
	})
}

// ConferenceStateResponse (body member 12) acknowledges a state version.
type ConferenceStateResponse struct {
	CurrentVersion int64 // 1
}

func (m *ConferenceStateResponse) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) (err error) {
		if id == 1 && t == TypeI64 {
			m.CurrentVersion, err = r.i64()
			return
		}
		return r.skip(t)
	})
}

func (m *ConferenceStateResponse) encode(w *writer) { w.fieldI64(1, m.CurrentVersion) }

// Subscription asks the server for remote media (type 2 = DOMINANT_SPEAKER).
type Subscription struct {
	CName        string // 1
	VideoQuality int32  // 2.1
	Type         int32  // 3
	TrackID      string // 4
}

// SubscriptionRequest (body member 14).
type SubscriptionRequest struct {
	Subscriptions []Subscription // 1
}

func (m *SubscriptionRequest) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) error {
		if id != 1 || t != TypeList {
			return r.skip(t)
		}
		return r.structList(func() error {
			var s Subscription
			err := r.readStruct(func(id int16, t Type) (err error) {
				switch {
				case id == 1 && t == TypeBinary:
					s.CName, err = r.string()
				case id == 2 && t == TypeStruct:
					err = r.readStruct(func(id int16, t Type) (err error) {
						if id == 1 && t == TypeI32 {
							s.VideoQuality, err = r.i32()
							return
						}
						return r.skip(t)
					})
				case id == 3 && t == TypeI32:
					s.Type, err = r.i32()
				case id == 4 && t == TypeBinary:
					s.TrackID, err = r.string()
				default:
					err = r.skip(t)
				}
				return
			})
			m.Subscriptions = append(m.Subscriptions, s)
			return err
		})
	})
}

func (m *SubscriptionRequest) encode(w *writer) {
	w.fieldHeader(1, TypeList)
	w.listHeader(TypeStruct, len(m.Subscriptions))
	for _, s := range m.Subscriptions {
		w.structBegin()
		w.fieldString(1, s.CName)
		w.fieldStruct(2, func() { w.fieldI32(1, s.VideoQuality) })
		w.fieldI32(3, s.Type)
		w.optString(4, s.TrackID)
		w.structEnd()
	}
}

// DataMessageRequest (body member 17) relays an app message through the
// server, e.g. group-call "E2eeKey" messages.
type DataMessageRequest struct {
	Message *DataMessage // 1
}

func (m *DataMessageRequest) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) error {
		if id == 1 && t == TypeStruct {
			m.Message = &DataMessage{}
			return m.Message.decode(r)
		}
		return r.skip(t)
	})
}

// StateSyncMessage covers UpdateRequest/Response (30/31, client -> server
// topic writes) and NotifyRequest/Response (32/33, server -> client pushes;
// topic "batched_notify" carries several topics in SyncPayload).
type StateSyncMessage struct {
	Topic       string       // 2
	Version     int32        // 3
	Data        []byte       // 4 (UpdateRequest/NotifyRequest)
	SyncPayload *SyncPayload // 1 (UpdateRequest) or 5 (NotifyRequest)
	TopicID     int32        // 5 (UpdateRequest) or 6 (NotifyRequest)
}

func (m *StateSyncMessage) decode(r *reader, member int16) error {
	return r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 2 && t == TypeBinary:
			m.Topic, err = r.string()
		case id == 3 && t == TypeI32:
			m.Version, err = r.i32()
		case id == 4 && t == TypeBinary:
			m.Data, err = r.binary()
		case t == TypeStruct && ((member == 30 && id == 1) || (member == 32 && id == 5)):
			m.SyncPayload, err = decodeSyncPayload(r)
		case t == TypeI32 && ((member == 30 && id == 5) || (member == 32 && id == 6)):
			m.TopicID, err = r.i32()
		default:
			err = r.skip(t)
		}
		return
	})
}

type stateSyncEncoder struct {
	m      *StateSyncMessage
	member int16
}

func (e stateSyncEncoder) encode(w *writer) {
	m := e.m
	sp := m.SyncPayload
	if sp == nil {
		sp = &SyncPayload{}
	}
	if e.member == 30 {
		w.syncPayload(1, sp)
	}
	w.fieldString(2, m.Topic)
	w.fieldI32(3, m.Version)
	switch e.member {
	case 30:
		w.fieldBinary(4, m.Data)
		w.fieldI32(5, m.TopicID)
	case 32:
		w.fieldBinary(4, m.Data)
		w.syncPayload(5, sp)
		w.fieldI32(6, m.TopicID)
	}
}

// ConnectMessage covers ConnectRequest/ConnectResponse (34/35). Not seen in
// the Messenger capture; used by the ULLC (AI/bot) call flavour.
type ConnectMessage struct {
	SDP              *SessionDescription // 1
	SDPOriginLocalID string              // 2 (response)
}

func (m *ConnectMessage) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeStruct:
			m.SDP, err = decodeSD(r)
		case id == 2 && t == TypeBinary:
			m.SDPOriginLocalID, err = r.string()
		default:
			err = r.skip(t)
		}
		return
	})
}

// ClientEvent reports a client-side milestone (type 1 = MEDIA_CONNECTED).
type ClientEvent struct {
	Type ClientEventType // 1
	Time int64           // 2 (ms since epoch)
}

// ClientEventRequest (body member 36).
type ClientEventRequest struct {
	Events []ClientEvent // 1
}

func (m *ClientEventRequest) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) error {
		if id != 1 || t != TypeList {
			return r.skip(t)
		}
		return r.structList(func() error {
			var ev ClientEvent
			err := r.readStruct(func(id int16, t Type) (err error) {
				switch {
				case id == 1 && t == TypeI32:
					var v int32
					v, err = r.i32()
					ev.Type = ClientEventType(v)
				case id == 2 && t == TypeI64:
					ev.Time, err = r.i64()
				default:
					err = r.skip(t)
				}
				return
			})
			m.Events = append(m.Events, ev)
			return err
		})
	})
}

func (m *ClientEventRequest) encode(w *writer) {
	w.fieldHeader(1, TypeList)
	w.listHeader(TypeStruct, len(m.Events))
	for _, ev := range m.Events {
		w.structBegin()
		w.fieldI32(1, int32(ev.Type))
		if ev.Time != 0 {
			w.fieldI64(2, ev.Time)
		}
		w.structEnd()
	}
}

func (m *ServerMediaUpdateRequest) encode(w *writer) {
	w.fieldI64(1, m.FromVersion)
	w.fieldI64(2, m.ToVersion)
	w.fieldHeader(3, TypeList)
	w.listHeader(TypeStruct, 0)
	w.optSD(4, m.Offer)
	w.optSD(6, m.Answer)
	if m.MediaStatus != nil {
		w.mediaStatus(7, m.MediaStatus)
	}
	w.fieldBool(8, m.RenegotiationRequested)
	w.optSD(9, m.PrAnswer)
	if m.StateStore != nil {
		w.fieldHeader(10, TypeMap)
		w.stateStoreValue(m.StateStore)
	}
	w.optString(11, m.SDPOriginLocalID)
	w.fieldBool(13, m.MultipleVideoStreamsAllowed)
	w.optSD(14, m.RenegotiationOffer)
	w.fieldI32(15, int32(m.MediaPath))
	if m.Update != nil {
		w.sessionDescriptionUpdate(16, m.Update)
	}
	w.fieldBool(17, m.ScreenShareStreamAllowed)
}

func (m *RingRequest) encode(w *writer) {
	w.fieldString(1, m.Caller)
	w.fieldStringList(2, TypeSet, m.OtherParticipants)
	w.fieldI32(4, int32(m.RingType))
	w.optString(5, m.OfferedExperiments)
	w.fieldBool(6, m.IsScheduledCall)
	if len(m.AppMessages) > 0 {
		w.dataMessages(8, m.AppMessages)
	}
	w.optSD(10, m.Offer)
	if m.MediaStatusEx != nil {
		w.mediaStatus(11, m.MediaStatusEx)
	}
	w.fieldBool(12, m.IsPreconnectSupported)
	w.optString(13, m.SDPOriginLocalID)
	w.optSD(14, m.UnifiedOffer)
	w.fieldI32(15, int32(m.MediaPath))
	if m.E2eeEnforcement != nil {
		w.enforcement(16, m.E2eeEnforcement)
	}
	w.fieldBool(17, m.IsLegacyCall)
	w.fieldBool(18, m.IsTransferCall)
	if m.RelayInfo != nil {
		w.fieldStruct(20, func() { m.RelayInfo.encode(w) })
	}
	w.optString(23, m.CallerClientSessionID)
	if m.ThreadGroupID != "" || m.ThreadPeerID != "" {
		w.fieldStruct(24, func() {
			w.optString(1, m.ThreadGroupID)
			w.optString(2, m.ThreadPeerID)
		})
	}
	w.optString(25, m.LinkURL)
}

func (m *ConferenceStateRequest) encode(w *writer) {
	w.fieldI64(1, m.Version)
	w.fieldHeader(2, TypeMap)
	w.mapHeader(TypeBinary, TypeStruct, len(m.ParticipantStates))
	for _, k := range slices.Sorted(maps.Keys(m.ParticipantStates)) {
		ps := m.ParticipantStates[k]
		w.binaryValue([]byte(k))
		w.structBegin()
		w.fieldI32(1, int32(ps.State))
		w.fieldString(2, ps.UserCapabilities)
		if ps.SCTPNodeID != 0 {
			w.fieldI64(3, ps.SCTPNodeID)
		}
		w.structEnd()
	}
	if len(m.AppMessages) > 0 {
		w.dataMessages(4, m.AppMessages)
	}
	w.fieldHeader(5, TypeList)
	w.listHeader(TypeStruct, 0)
}

func (m *DataMessageRequest) encode(w *writer) {
	if m.Message != nil {
		w.fieldStruct(1, func() { m.Message.encode(w) })
	}
}

func (m *ConnectMessage) encode(w *writer) {
	w.optSD(1, m.SDP)
	w.optString(2, m.SDPOriginLocalID)
}

// DataMessageResponse (body member 19) acknowledges a DATA_MESSAGE. The
// server fills deliveryResult with per-recipient status codes; clients send
// an empty map.
type DataMessageResponse struct {
	DeliveryResult map[string]int32 // 1
	hasServiceMap  bool             // 2 (serviceTypeDeliveryResult, not modelled)
}

func (m *DataMessageResponse) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeMap:
			m.DeliveryResult = map[string]int32{}
			err = r.stringMap(func(k string, vt Type) error {
				if vt != TypeI32 {
					return r.skip(vt)
				}
				v, err := r.i32()
				m.DeliveryResult[k] = v
				return err
			})
		case id == 2 && t == TypeMap:
			m.hasServiceMap = true
			err = r.skip(t)
		default:
			err = r.skip(t)
		}
		return
	})
}

func (m *DataMessageResponse) encode(w *writer) {
	w.fieldHeader(1, TypeMap)
	if len(m.DeliveryResult) == 0 {
		w.mapHeader(0, 0, 0)
	} else {
		w.mapHeader(TypeBinary, TypeI32, len(m.DeliveryResult))
		for _, k := range slices.Sorted(maps.Keys(m.DeliveryResult)) {
			w.binaryValue([]byte(k))
			w.zigzag(int64(m.DeliveryResult[k]))
		}
	}
	if m.hasServiceMap {
		w.fieldHeader(2, TypeMap)
		w.mapHeader(0, 0, 0)
	}
}

func (ri *RelayInfo) encode(w *writer) {
	w.fieldHeader(1, TypeList)
	w.listHeader(TypeStruct, len(ri.Turns))
	for _, t := range ri.Turns {
		w.structBegin()
		w.fieldBinary(1, addrBytes(t.IPv4, net.IPv4len))
		if t.IPv6 != "" {
			w.fieldBinary(2, addrBytes(t.IPv6, net.IPv6len))
		}
		w.fieldI32(3, t.UDPPort)
		if t.TCPPort != 0 {
			w.fieldI32(4, t.TCPPort)
		}
		if t.SSLTCPPort != 0 {
			w.fieldI32(5, t.SSLTCPPort)
		}
		if t.TLSPort != 0 {
			w.fieldI32(7, t.TLSPort)
		}
		w.optString(9, t.TurnUsername)
		w.optString(10, t.TurnPassword)
		w.structEnd()
	}
	w.optString(3, ri.TurnUsername)
	w.optString(4, ri.TurnPassword)
}

func addrBytes(s string, n int) []byte {
	ip := net.ParseIP(s)
	if ip == nil {
		return []byte(s)
	}
	if n == net.IPv4len {
		if v4 := ip.To4(); v4 != nil {
			return v4
		}
	}
	return ip.To16()
}
