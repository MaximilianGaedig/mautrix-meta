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
	"strings"
	"testing"

	"go.mau.fi/libsignal/ecc"
)

func serializeKey(k ecc.ECPublicKeyable) []byte {
	b := k.Serialize()
	return b[:]
}

// fakeBundle builds a pre-key bundle with freshly generated keys.
func fakeBundle(t *testing.T) (*PreKeyBundle, *ecc.ECKeyPair) {
	t.Helper()
	ik, err := ecc.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	spk, _ := ecc.GenerateKeyPair()
	pk, _ := ecc.GenerateKeyPair()
	spkPub := serializeKey(spk.PublicKey())
	sig := ecc.CalculateSignature(ik.PrivateKey(), spkPub)
	return &PreKeyBundle{
		IdentityKey:     serializeKey(ik.PublicKey()),
		SignedPreKeyID:  1,
		SignedPreKey:    spkPub,
		SignedPreKeySig: sig[:],
		PreKeyID:        1,
		PreKey:          serializeKey(pk.PublicKey()),
	}, ik
}

func TestPreKeyBundle(t *testing.T) {
	b, _ := fakeBundle(t)
	got, err := ParsePreKeyBundle(b.Marshal())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.IdentityKey, b.IdentityKey) || got.SignedPreKeyID != 1 || !bytes.Equal(got.PreKey, b.PreKey) {
		t.Fatal("bundle round trip mismatch")
	}
	if ok, err := got.VerifySignedPreKey(); err != nil || !ok {
		t.Fatalf("signature did not verify: %v", err)
	}
	got.SignedPreKey = bytes.Clone(got.SignedPreKey)
	got.SignedPreKey[5] ^= 1
	if ok, _ := got.VerifySignedPreKey(); ok {
		t.Fatal("tampered pre-key verified")
	}
}

func TestE2eeClientStateRoundTrip(t *testing.T) {
	b, _ := fakeBundle(t)
	cs := &E2eeClientState{
		PreKeyBundle:       b.Marshal(),
		CipherSuites:       []int16{2},
		SupportedVersions:  []VersionRange{{Min: 4, Max: 7}},
		AllowOptionalGfd:   true,
		IdentityKeyMode:    2,
		DeviceID:           7,
		KeyNegotiationProt: []int32{0},
	}
	data := cs.Marshal()
	got, err := ParseE2eeClientState(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.DeviceID != 7 || got.IdentityKeyMode != 2 || !got.AllowOptionalGfd ||
		got.SupportedVersions[0] != (VersionRange{4, 7}) || !bytes.Equal(got.PreKeyBundle, cs.PreKeyBundle) {
		t.Fatalf("client state mismatch: %+v", got)
	}
	if !bytes.Equal(got.Marshal(), data) {
		t.Fatal("client state not byte-stable")
	}

	// It rides in the JOIN's syncPayload.
	payload, _ := EncodePayload(fakeJoin(data))
	msg, err := DecodePayload(payload, false)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := msg.Body.JoinRequest.SyncPayload.StateStore.Get(TopicE2eeState)
	if !bytes.Equal(st.Data, data) {
		t.Fatal("E2eeState lost in JOIN")
	}
}

func TestE2eeServerState(t *testing.T) {
	// Empty server state as seen for P2P calls: default config only.
	w := &writer{}
	w.structBegin()
	w.fieldHeader(2, TypeList)
	w.listHeader(TypeI16, 0)
	w.versionRanges(3, nil)
	w.fieldStruct(4, func() { w.fieldI16(1, 0) })
	w.fieldHeader(5, TypeMap)
	w.mapHeader(0, 0, 0)
	w.fieldI32(7, 0)
	w.structEnd()
	ss, err := ParseE2eeServerState(w.bytes())
	if err != nil || len(ss.EndpointInfos) != 0 {
		t.Fatalf("empty state: %v %+v", err, ss)
	}

	// Server state with one remote endpoint, as expected for SFU calls.
	b, _ := fakeBundle(t)
	w = &writer{}
	w.structBegin()
	w.fieldHeader(5, TypeMap)
	w.mapHeader(TypeBinary, TypeStruct, 1)
	w.binaryValue([]byte(fakeCallee + ":3"))
	w.structBegin()
	w.fieldBinary(1, b.Marshal())
	w.fieldStruct(2, func() { w.fieldI16(1, 2) })
	w.fieldI32(3, 3)
	w.structEnd()
	w.fieldI32(7, 1)
	w.structEnd()
	ss, err = ParseE2eeServerState(w.bytes())
	if err != nil {
		t.Fatal(err)
	}
	ei, ok := ss.EndpointInfos[fakeCallee+":3"]
	if !ok || ei.DeviceID != 3 || ei.IdentityKeyMode != 2 || ss.NegotiatedProtocol != 1 {
		t.Fatalf("endpoint info mismatch: %+v", ss)
	}
	if uid, ok := EndpointUserID(fakeCallee + ":3"); !ok || uid != 100000000000002 {
		t.Fatal("endpoint user id")
	}
}

func TestDtlsAuth(t *testing.T) {
	_, ik := fakeBundle(t)
	sig := ecc.CalculateSignature(ik.PrivateKey(), []byte("placeholder"))
	info := &DtlsAuthInfo{ProtocolVersion: 1, IdentityKeyMode: 2, DeviceID: 7, PublicKey: serializeKey(ik.PublicKey()), Signature: sig[:]}
	attr := info.String()

	sdp := AddDtlsAuth(fakeOfferSDP, attr)
	if !strings.Contains(sdp, "a=group:BUNDLE 0 1 2\r\na=x-dtls-auth:"+attr+"\r\nm=audio") {
		t.Fatal("attribute not inserted after the session-level group line")
	}
	if SDPDtlsAuth(sdp) != attr {
		t.Fatal("attribute not found")
	}
	if StripDtlsAuth(sdp) != fakeOfferSDP {
		t.Fatal("strip did not restore the original SDP")
	}
	got, err := ParseDtlsAuthInfo(SDPDtlsAuth(sdp))
	if err != nil {
		t.Fatal(err)
	}
	if got.DeviceID != 7 || got.IdentityKeyMode != 2 || !bytes.Equal(got.PublicKey, info.PublicKey) || got.String() != attr {
		t.Fatalf("dtls auth mismatch: %+v", got)
	}
	if fps := SDPFingerprints(sdp); len(fps) != 1 || fps[0] != fakeFingerprint {
		t.Fatalf("fingerprints: %v", fps)
	}
}

func TestServerInfoData(t *testing.T) {
	sid, err := ParseServerInfoData(fakeServerInfo)
	if err != nil {
		t.Fatal(err)
	}
	if sid.Region != "abc" || sid.CallKey != "AAAABBBBCCCCDDDD" || sid.Number != 42 || sid.String() != fakeServerInfo {
		t.Fatalf("server info mismatch: %+v", sid)
	}
}
