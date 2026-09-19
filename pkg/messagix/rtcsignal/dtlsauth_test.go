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
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"

	"go.mau.fi/libsignal/ecc"

	"go.mau.fi/mautrix-meta/pkg/messagix/dgw"
)

func TestDtlsAuthMessageLayout(t *testing.T) {
	pub := append([]byte{5}, bytes.Repeat([]byte{0xAA}, 32)...)
	got := DtlsAuthMessage(100000000000001, 3, 2, "sha-256", "AB:CD", pub)
	want := append([]byte("dtls_authentication_context10000000000000132sha-256AB:CD"), pub...)
	if !bytes.Equal(got, want) {
		t.Fatalf("message layout:\n got %q\nwant %q", got, want)
	}
}

func TestSignVerifyDTLSAuth(t *testing.T) {
	ik, err := ecc.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	const uid, dev = int64(100000000000001), int32(7)
	priv := ik.PrivateKey().Serialize()
	pub := ik.PublicKey().PublicKey() // 32 raw bytes; the signer adds the 0x05 prefix

	info, err := SignDTLSAuth(priv, pub[:], uid, dev, fakeOfferSDP)
	if err != nil {
		t.Fatal(err)
	}
	if info.ProtocolVersion != 1 || info.IdentityKeyMode != 2 || info.DeviceID != dev || len(info.PublicKey) != 33 {
		t.Fatalf("unexpected info: %+v", info)
	}
	sdp := AddDtlsAuth(fakeOfferSDP, info.String())
	serialized := ik.PublicKey().Serialize()

	if _, err = VerifyDTLSAuth(sdp, uid, serialized); err != nil {
		t.Fatalf("verify own signature: %v", err)
	}
	if _, err = VerifyDTLSAuth(sdp, uid, nil); err != nil {
		t.Fatalf("verify without expected key: %v", err)
	}
	if _, err = VerifyDTLSAuth(sdp, uid+1, serialized); !errors.Is(err, ErrDtlsAuthSignature) {
		t.Fatalf("wrong user id: got %v", err)
	}
	other, _ := ecc.GenerateKeyPair()
	if _, err = VerifyDTLSAuth(sdp, uid, other.PublicKey().Serialize()); !errors.Is(err, ErrDtlsAuthKeyMismatch) {
		t.Fatalf("wrong identity key: got %v", err)
	}
	// Another fingerprint (a different DTLS certificate) must not verify.
	algo, digest, _ := strings.Cut(fakeFingerprint, " ")
	swapped := strings.ReplaceAll(sdp, digest, "FF"+digest[2:])
	if _, err = VerifyDTLSAuth(swapped, uid, serialized); !errors.Is(err, ErrDtlsAuthSignature) {
		t.Fatalf("changed fingerprint: got %v", err)
	}
	if err = info.Verify(uid, algo, digest, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = VerifyDTLSAuth(fakeOfferSDP, uid, nil); !errors.Is(err, ErrDtlsAuthMissing) {
		t.Fatalf("missing attribute: got %v", err)
	}
}

// TestDtlsAuthHAR verifies the real x-dtls-auth attributes of a captured call:
// the caller's (JOIN offer, signed by the sender) and the callee's (SMU
// answer, signed by sdpOriginLocalId). Skipped unless RTCSIGNAL_HAR is set;
// logs only pass/fail.
func TestDtlsAuthHAR(t *testing.T) {
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
					Type   string `json:"type"`
					Opcode int    `json:"opcode"`
					Data   string `json:"data"`
				} `json:"_webSocketMessages"`
			} `json:"entries"`
		} `json:"log"`
	}
	if err = json.Unmarshal(raw, &har); err != nil {
		t.Fatal(err)
	}
	checked := 0
	check := func(what, sdp, uidStr string) {
		uid, err := strconv.ParseInt(uidStr, 10, 64)
		if err != nil || SDPDtlsAuth(sdp) == "" {
			return
		}
		checked++
		if _, err = VerifyDTLSAuth(sdp, uid, nil); err != nil {
			t.Errorf("%s: %v", what, err)
		} else {
			t.Logf("%s: x-dtls-auth verifies", what)
		}
		if _, err = VerifyDTLSAuth(sdp, uid+1, nil); err == nil {
			t.Errorf("%s: verifies with the wrong user id", what)
		}
	}
	for _, e := range har.Log.Entries {
		if !strings.Contains(e.Request.URL, RPSignalingPath) {
			continue
		}
		for _, m := range e.WebSocketMessages {
			if m.Opcode != 2 {
				continue
			}
			data, err := base64.StdEncoding.DecodeString(m.Data)
			if err != nil {
				continue
			}
			events, _ := DecodeWebsocketMessage(data, m.Type == "receive")
			for _, ev := range events {
				if _, ok := ev.Frame.(*dgw.DataFrame); !ok || ev.Err != nil || ev.Message == nil {
					continue
				}
				msg := ev.Message
				if j := msg.Body.JoinRequest; j != nil && j.Offer != nil {
					check("caller offer (JOIN)", j.Offer.SDP, msg.Header.SenderID)
				}
				if s := msg.Body.ServerMediaUpdateRequest; s != nil && s.Answer != nil {
					check("callee answer (SMU)", s.Answer.SDP, s.SDPOriginLocalID)
				}
			}
		}
	}
	if checked != 2 {
		t.Fatalf("expected 2 signed SDPs, found %d", checked)
	}
}
