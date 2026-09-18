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

// TestDecodeHAR decodes every rpsignaling frame of a browser HAR capture.
// It is skipped unless RTCSIGNAL_HAR points at a HAR file, and it only logs
// message types and body members, never payload contents, so it is safe to
// run against captures of real calls.
func TestDecodeHAR(t *testing.T) {
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
	var messages, reencoded, clientMsgs int
	for _, e := range har.Log.Entries {
		if !strings.Contains(e.Request.URL, RPSignalingPath) {
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
				messages++
				msg := ev.Message
				t.Logf("%7.3f %-7s %-19s resp=%-5t body=%s", m.Time-start, m.Type, msg.Header.Type, msg.Header.IsResponse(), msg.Body.Name())
				if !fromServer {
					clientMsgs++
					enc, err := EncodePayload(msg)
					if err != nil {
						t.Errorf("message %d: re-encode: %v", i, err)
					} else if bytes.Equal(enc, df.Payload) {
						reencoded++
					} else {
						t.Logf("message %d (%s/%s) re-encodes differently", i, msg.Header.Type, msg.Body.Name())
					}
				}
			}
		}
	}
	if messages == 0 {
		t.Fatal("no rpsignaling messages found")
	}
	t.Logf("%d messages decoded, %d/%d client messages re-encode byte-exactly", messages, reencoded, clientMsgs)
}
