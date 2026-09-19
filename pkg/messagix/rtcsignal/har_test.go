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
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix/dgw"
)

// harMessage is one multiway message found in a HAR capture.
type harMessage struct {
	Time       float64 // seconds since the socket's first message
	Dir        string  // "send" or "receive"
	Transport  string  // "dgw" (rpsignaling) or "mqtt" (/t_rtc_multi)
	Msg        *Message
	RawPayload []byte // the exact bytes the message was decoded from
}

// loadHARMessages decodes every rpsignaling data frame and every
// /t_rtc_multi MQTT publish of the HAR named by RTCSIGNAL_HAR. It skips the
// test if the variable is unset.
func loadHARMessages(t *testing.T) []harMessage {
	t.Helper()
	path := os.Getenv("RTCSIGNAL_HAR")
	if path == "" {
		t.Skip("RTCSIGNAL_HAR not set")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var har struct {
		Log struct {
			Entries []struct {
				Request struct {
					URL string `json:"url"`
				} `json:"request"`
				WebSocketMessages []struct {
					Type   string  `json:"type"`
					Time   float64 `json:"time"`
					Opcode int     `json:"opcode"`
					Data   string  `json:"data"`
				} `json:"_webSocketMessages"`
			} `json:"entries"`
		} `json:"log"`
	}
	if err = json.Unmarshal(raw, &har); err != nil {
		t.Fatal(err)
	}
	var out []harMessage
	for _, e := range har.Log.Entries {
		isDGW := strings.Contains(e.Request.URL, RPSignalingPath)
		isMQTT := strings.Contains(e.Request.URL, "edge-chat.facebook.com/chat")
		if !isDGW && !isMQTT {
			continue
		}
		start := 0.0
		for i, m := range e.WebSocketMessages {
			if start == 0 {
				start = m.Time
			}
			if m.Opcode != 2 {
				continue
			}
			data, err := base64.StdEncoding.DecodeString(m.Data)
			if err != nil {
				t.Fatalf("message %d: %v", i, err)
			}
			fromServer := m.Type == "receive"
			if isMQTT {
				topic, payload, ok := parseMQTTPublish(data)
				if !ok || topic != MQTTTopic {
					continue
				}
				_, msg, err := DecodeMQTTPayload(payload)
				if err != nil {
					t.Fatalf("mqtt message %d: %v", i, err)
				}
				out = append(out, harMessage{m.Time - start, m.Type, "mqtt", msg, payload})
				continue
			}
			events, err := DecodeWebsocketMessage(data, fromServer)
			if err != nil {
				t.Fatalf("message %d: %v", i, err)
			}
			for _, ev := range events {
				df, ok := ev.Frame.(*dgw.DataFrame)
				if !ok {
					continue
				}
				if ev.Err != nil {
					t.Fatalf("message %d: %v", i, ev.Err)
				}
				out = append(out, harMessage{m.Time - start, m.Type, "dgw", ev.Message, df.Payload})
			}
		}
	}
	return out
}

// parseMQTTPublish extracts topic and payload from an MQTT 3.1 PUBLISH.
func parseMQTTPublish(b []byte) (topic string, payload []byte, ok bool) {
	if len(b) < 2 || b[0]>>4 != 3 {
		return "", nil, false
	}
	qos := (b[0] >> 1) & 3
	p, length, mul := 1, 0, 1
	for {
		if p >= len(b) {
			return "", nil, false
		}
		c := b[p]
		p++
		length += int(c&0x7f) * mul
		mul *= 128
		if c < 0x80 {
			break
		}
	}
	if p+length != len(b) || p+2 > len(b) {
		return "", nil, false
	}
	tl := int(b[p])<<8 | int(b[p+1])
	p += 2
	if p+tl > len(b) {
		return "", nil, false
	}
	topic = string(b[p : p+tl])
	p += tl
	if qos > 0 {
		p += 2
	}
	if p > len(b) {
		return "", nil, false
	}
	return topic, b[p:], true
}

// TestDecodeHAR decodes every rpsignaling frame and /t_rtc_multi publish of
// a browser HAR capture and requires every client message to re-encode
// byte for byte. It is skipped unless RTCSIGNAL_HAR points at a HAR file,
// and it only logs message types and body members, never payload contents,
// so it is safe to run against captures of real calls.
func TestDecodeHAR(t *testing.T) {
	msgs := loadHARMessages(t)
	var reencoded, clientMsgs int
	for i, hm := range msgs {
		msg := hm.Msg
		t.Logf("%7.3f %-4s %-7s %-19s resp=%-5t body=%s", hm.Time, hm.Transport, hm.Dir, msg.Header.Type, msg.Header.IsResponse(), msg.Body.Name())
		if hm.Dir != "send" || hm.Transport != "dgw" {
			continue
		}
		clientMsgs++
		enc, err := EncodePayload(msg)
		if err != nil {
			t.Errorf("message %d: re-encode: %v", i, err)
		} else if bytes.Equal(enc, hm.RawPayload) {
			reencoded++
		} else {
			t.Errorf("message %d (%s/%s) re-encodes differently", i, msg.Header.Type, msg.Body.Name())
		}
	}
	if len(msgs) == 0 {
		t.Fatal("no rpsignaling messages found")
	}
	t.Logf("%d messages decoded, %d/%d client messages re-encode byte-exactly", len(msgs), reencoded, clientMsgs)
}
