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

	"go.mau.fi/mautrix-meta/pkg/messagix/dgw"
)

// Service is RtcdgwserviceTypes.Service, the unified-stream routing key.
type Service int32

const (
	ServiceUnknown Service = 0
	ServiceMWS     Service = 1 // multiway signalling (calls)
	ServiceGenAI   Service = 2
	ServiceLiveAI  Service = 3
	ServiceGraphQL Service = 4
	ServiceACP     Service = 5
)

// Envelope is the unified-stream wrapper around every message on the
// rpsignaling socket (x-dgw-app-useUnifiedStream=true): Rtcdgwservice
// Request (client -> server: 1 payload, 2 serviceType, 4 compressedPayload)
// or Response (server -> client: 1 payload, 2 serviceType, 3 error,
// 5 compressedPayload). The payload is a multiway Message.
type Envelope struct {
	Payload     []byte
	ServiceType Service
	// Error is ErrorResponse.errorMessage (responses only).
	Error string
	// Compressed is set if a compressedPayload was present; its encoding
	// (compressionVersion) was never seen and is not handled.
	Compressed         []byte
	CompressionVersion int64
}

// ErrCompressed is returned when an envelope only has a compressed payload.
var ErrCompressed = errors.New("rtcsignal: compressed unified-stream payloads are not supported")

// ParseEnvelope decodes a unified-stream Request (fromServer=false) or
// Response (fromServer=true).
func ParseEnvelope(data []byte, fromServer bool) (*Envelope, error) {
	env := &Envelope{}
	compressedID := int16(4)
	if fromServer {
		compressedID = 5
	}
	r := newReader(data)
	err := r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeBinary:
			env.Payload, err = r.binary()
		case id == 2 && t == TypeI32:
			var v int32
			v, err = r.i32()
			env.ServiceType = Service(v)
		case id == 3 && t == TypeStruct && fromServer:
			err = r.readStruct(func(id int16, t Type) (err error) {
				if id == 1 && t == TypeBinary {
					env.Error, err = r.string()
					return
				}
				return r.skip(t)
			})
		case id == compressedID && t == TypeStruct:
			err = r.readStruct(func(id int16, t Type) (err error) {
				switch {
				case id == 1 && t == TypeBinary:
					env.Compressed, err = r.binary()
				case id == 2 && t == TypeI64:
					env.CompressionVersion, err = r.i64()
				default:
					err = r.skip(t)
				}
				return
			})
		default:
			err = r.skip(t)
		}
		return
	})
	if err != nil {
		return nil, fmt.Errorf("rtcsignal: envelope: %w", err)
	}
	if r.remaining() != 0 {
		return nil, fmt.Errorf("rtcsignal: envelope: %d trailing bytes", r.remaining())
	}
	return env, nil
}

// Marshal encodes the envelope as a Request (client side).
func (e *Envelope) Marshal() []byte {
	w := &writer{}
	w.structBegin()
	w.fieldBinary(1, e.Payload)
	w.fieldI32(2, int32(e.ServiceType))
	w.structEnd()
	return w.bytes()
}

// MarshalResponse encodes the envelope as a server Response (for tests and
// fixtures).
func (e *Envelope) MarshalResponse() []byte {
	w := &writer{}
	w.structBegin()
	w.fieldBinary(1, e.Payload)
	w.fieldI32(2, int32(e.ServiceType))
	if e.Error != "" {
		w.fieldStruct(3, func() { w.fieldString(1, e.Error) })
	}
	w.structEnd()
	return w.bytes()
}

// DecodePayload unwraps a DGW data frame payload into a multiway message.
func DecodePayload(data []byte, fromServer bool) (*Message, error) {
	env, err := ParseEnvelope(data, fromServer)
	if err != nil {
		return nil, err
	}
	if env.Error != "" {
		return nil, fmt.Errorf("rtcsignal: unified stream server error: %s", env.Error)
	}
	if env.Payload == nil {
		if env.Compressed != nil {
			return nil, ErrCompressed
		}
		return nil, errors.New("rtcsignal: envelope without payload")
	}
	if env.ServiceType != ServiceMWS {
		return nil, fmt.Errorf("rtcsignal: unexpected service type %d", env.ServiceType)
	}
	return Unmarshal(env.Payload)
}

// EncodePayload wraps a message for sending as a DGW data frame payload.
func EncodePayload(msg *Message) ([]byte, error) {
	b, err := msg.Marshal()
	if err != nil {
		return nil, err
	}
	return (&Envelope{Payload: b, ServiceType: ServiceMWS}).Marshal(), nil
}

// Stream parameters observed for wss://gateway.facebook.com/ws/rpsignaling.
// Unlike /ws/lightspeed, the stream-group headers go in the URL query
// (x-dgw-app-stream-group=group1, x-dgw-app-useUnifiedStream=true, plus
// x-dgw-loggingid) and the single stream (id 0) is established with "{}",
// answered by {"code":200}. All signalling then flows as DGW data frames on
// that stream with RequiresAck set; each side acks every data frame with an
// AckFrame carrying the same ack id. Ack ids count per direction. The
// socket-level keepalive is the usual 1-byte Ping/Pong.
const (
	RPSignalingPath           = "/ws/rpsignaling"
	RPSignalingStreamGroup    = "group1"
	RPSignalingEstablishParam = "{}"
)

// FrameEvent is one decoded DGW frame from an rpsignaling websocket message.
type FrameEvent struct {
	Frame   dgw.Frame
	Message *Message // set for data frames carrying a multiway message
	Err     error    // decode error for the data frame payload, if any
}

// DecodeWebsocketMessage splits one binary websocket message into DGW frames
// (several may be concatenated) and decodes the multiway messages in data
// frames. fromServer selects Response vs Request envelopes.
func DecodeWebsocketMessage(data []byte, fromServer bool) ([]FrameEvent, error) {
	var out []FrameEvent
	for len(data) > 0 {
		frame := dgw.CheckFrameType(data)
		rest, err := frame.Unmarshal(data)
		if err != nil {
			return out, fmt.Errorf("rtcsignal: DGW frame: %w", err)
		}
		ev := FrameEvent{Frame: frame}
		if df, ok := frame.(*dgw.DataFrame); ok {
			ev.Message, ev.Err = DecodePayload(df.Payload, fromServer)
		}
		out = append(out, ev)
		data = rest
	}
	return out, nil
}
