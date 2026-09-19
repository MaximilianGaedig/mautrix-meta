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

// MQTTTopic is the edge-chat MQTT topic that carries multiway messages to
// idle devices. In the callee capture the RING for an incoming call arrived
// here (QoS 1, retain flag set) on a dedicated edge-chat connection that
// subscribes to nothing else, not on the rpsignaling socket.
const MQTTTopic = "/t_rtc_multi"

// MQTTHeader is MqttThriftHeader, which precedes the multiway message on
// /t_rtc_multi. It was empty (a single stop byte) in the capture.
type MQTTHeader struct {
	TraceInfo            string // 1
	CoreContextRequestID string // 2
}

// DecodeMQTTPayload decodes a /t_rtc_multi publish payload:
// MqttThriftHeader followed by the multiway message.
func DecodeMQTTPayload(data []byte) (*MQTTHeader, *Message, error) {
	r := newReader(data)
	hdr := &MQTTHeader{}
	err := r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeBinary:
			hdr.TraceInfo, err = r.string()
		case id == 2 && t == TypeBinary:
			hdr.CoreContextRequestID, err = r.string()
		default:
			err = r.skip(t)
		}
		return
	})
	if err != nil {
		return nil, nil, fmt.Errorf("rtcsignal: MqttThriftHeader: %w", err)
	}
	msg, err := Unmarshal(data[r.p:])
	return hdr, msg, err
}

// EncodeMQTTPayload is the inverse of DecodeMQTTPayload (used for fixtures).
func EncodeMQTTPayload(hdr *MQTTHeader, msg *Message) ([]byte, error) {
	w := &writer{}
	w.structBegin()
	if hdr != nil {
		w.optString(1, hdr.TraceInfo)
		w.optString(2, hdr.CoreContextRequestID)
	}
	w.structEnd()
	body, err := msg.Marshal()
	if err != nil {
		return nil, err
	}
	return append(w.bytes(), body...), nil
}
