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
	"errors"
	"strconv"
	"strings"
	"testing"

	"go.mau.fi/libsignal/ecc"
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

// TestDtlsAuthHAR re-verifies every x-dtls-auth attribute in a capture:
// JOIN offers/answers (signed by the header sender), SMU answers (signed by
// sdpOriginLocalId) and RING offers (signed by the caller, delivered over
// rpsignaling or /t_rtc_multi). Skipped unless RTCSIGNAL_HAR is set; logs
// only pass/fail.
func TestDtlsAuthHAR(t *testing.T) {
	msgs := loadHARMessages(t)
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
	for _, hm := range msgs {
		msg := hm.Msg
		if j := msg.Body.JoinRequest; j != nil {
			if j.Offer != nil && j.Offer.SDP != "" {
				check("JOIN offer", j.Offer.SDP, msg.Header.SenderID)
				checkBundleKey(t, j, j.Offer.SDP)
			}
			if j.Answer != nil {
				check("JOIN answer", j.Answer.SDP, msg.Header.SenderID)
			}
		}
		if s := msg.Body.ServerMediaUpdateRequest; s != nil && s.Answer != nil {
			check("SMU answer", s.Answer.SDP, s.SDPOriginLocalID)
		}
		if r := msg.Body.RingRequest; r != nil && r.Offer != nil {
			check("RING offer via "+hm.Transport, r.Offer.SDP, r.Caller)
		}
	}
	if checked == 0 {
		t.Fatal("no signed SDPs found")
	}
	t.Logf("%d signed SDPs verified", checked)
}

// checkBundleKey compares the x-dtls-auth key of a JOIN's own SDP with the
// identity key in the same JOIN's E2eeState pre-key bundle.
func checkBundleKey(t *testing.T, j *JoinRequest, sdp string) {
	if j.SyncPayload == nil {
		return
	}
	st, ok := j.SyncPayload.StateStore.Get(TopicE2eeState)
	if !ok {
		return
	}
	cs, err := ParseE2eeClientState(st.Data)
	if err != nil {
		t.Errorf("E2eeClientState: %v", err)
		return
	}
	bundle, err := ParsePreKeyBundle(cs.PreKeyBundle)
	if err != nil {
		t.Errorf("preKeyBundle: %v", err)
		return
	}
	info, err := ParseDtlsAuthInfo(SDPDtlsAuth(sdp))
	if err != nil {
		t.Errorf("x-dtls-auth: %v", err)
		return
	}
	if !bytes.Equal(info.PublicKey, bundle.IdentityKey) || info.DeviceID != cs.DeviceID {
		t.Errorf("JOIN x-dtls-auth key/device does not match the E2eeState bundle")
	} else {
		t.Logf("JOIN x-dtls-auth key and device id match the E2eeState bundle")
	}
}
