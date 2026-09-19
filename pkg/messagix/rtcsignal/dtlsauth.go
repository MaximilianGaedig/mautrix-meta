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
	"fmt"
	"strconv"
	"strings"

	"go.mau.fi/libsignal/ecc"
)

// x-dtls-auth signing, recovered from the web client's frame-encryption WASM
// (DtlsAuthenticatorCurve25519; the message builder is the only function that
// references the context string) and checked against both real signatures of
// a captured call (the caller's offer and the callee's answer):
//
//	msg = "dtls_authentication_context"
//	    || decimal(userID) || decimal(deviceID) || decimal(identityKeyMode)
//	    || fingerprintAlgo || fingerprintDigest      // SDP text, e.g. "sha-256" "AB:CD:…", no separator
//	    || identityPublicKey                         // 33 bytes, 0x05 || X25519
//	signature = XEdDSA(identityPrivateKey, msg)      // Signal's curve25519 calculateSignature
//
// userID is the signer's Facebook user id (the verifier passes the remote
// user id it expects), deviceID and identityKeyMode are the values carried in
// IdentityKeyPublicInfo. The protocol version is not signed.

// DtlsAuthContext is the domain-separation prefix of the signed message.
const DtlsAuthContext = "dtls_authentication_context"

const (
	DtlsAuthProtocolVersion = 1
	// IdentityKeyModePersistent is ZenonIdentityKeyMode
	// USE_PERSISTENT_IDENTITY_KEYS_AND_VALIDATE, the mode of the MAW identity
	// store (the E2EE chat's Signal identity).
	IdentityKeyModePersistent = 2
)

var (
	ErrDtlsAuthMissing     = errors.New("rtcsignal: SDP has no x-dtls-auth attribute")
	ErrDtlsAuthFingerprint = errors.New("rtcsignal: SDP must have exactly one distinct DTLS fingerprint")
	ErrDtlsAuthKeyMismatch = errors.New("rtcsignal: x-dtls-auth key differs from the expected identity key")
	ErrDtlsAuthSignature   = errors.New("rtcsignal: x-dtls-auth signature is invalid")
)

// DtlsAuthMessage builds the byte string that x-dtls-auth signs.
func DtlsAuthMessage(userID int64, deviceID int32, mode int16, algo, digest string, publicKey []byte) []byte {
	var b bytes.Buffer
	b.WriteString(DtlsAuthContext)
	b.WriteString(strconv.FormatInt(userID, 10))
	b.WriteString(strconv.FormatInt(int64(deviceID), 10))
	b.WriteString(strconv.FormatInt(int64(mode), 10))
	b.WriteString(algo)
	b.WriteString(digest)
	b.Write(publicKey)
	return b.Bytes()
}

// sdpFingerprint returns the single distinct fingerprint of an SDP, as the
// web client requires for both signing and verifying.
func sdpFingerprint(sdp string) (algo, digest string, err error) {
	fps := SDPFingerprints(sdp)
	if len(fps) != 1 {
		return "", "", ErrDtlsAuthFingerprint
	}
	algo, digest, _ = strings.Cut(fps[0], " ")
	return algo, digest, nil
}

func normalizeIdentityKey(pub []byte) ([]byte, error) {
	switch {
	case len(pub) == 33 && pub[0] == ecc.DjbType:
		return pub, nil
	case len(pub) == 32:
		return append([]byte{ecc.DjbType}, pub...), nil
	default:
		return nil, errBadKey
	}
}

// SignDTLSAuth creates the x-dtls-auth attribute for a local SDP (offer or
// answer) that carries exactly one DTLS fingerprint. priv and pub are the
// device's Signal identity key pair (pub as 32 raw or 33 prefixed bytes),
// userID the own Facebook user id and deviceID the own Signal device id.
// Use AddDtlsAuth to insert the result into the SDP.
func SignDTLSAuth(priv [32]byte, pub []byte, userID int64, deviceID int32, sdp string) (*DtlsAuthInfo, error) {
	pub, err := normalizeIdentityKey(pub)
	if err != nil {
		return nil, err
	}
	algo, digest, err := sdpFingerprint(sdp)
	if err != nil {
		return nil, err
	}
	msg := DtlsAuthMessage(userID, deviceID, IdentityKeyModePersistent, algo, digest, pub)
	sig := ecc.CalculateSignature(ecc.NewDjbECPrivateKey(priv), msg)
	return &DtlsAuthInfo{
		ProtocolVersion: DtlsAuthProtocolVersion,
		IdentityKeyMode: IdentityKeyModePersistent,
		DeviceID:        deviceID,
		PublicKey:       pub,
		Signature:       sig[:],
	}, nil
}

// VerifyDTLSAuth checks the x-dtls-auth attribute of a remote SDP. userID is
// the remote Facebook user id the call is with. expectedKey, if non-nil, is
// the identity key the E2EE store holds for (userID, info.DeviceID) — the web
// client fetches it by the device id in the attribute and rejects a mismatch.
// With a nil expectedKey only the self-consistency of the attribute is
// checked, which proves nothing about who sent it. The parsed attribute is
// returned even when verification fails, so the caller can look up the key.
func VerifyDTLSAuth(sdp string, userID int64, expectedKey []byte) (*DtlsAuthInfo, error) {
	attr := SDPDtlsAuth(sdp)
	if attr == "" {
		return nil, ErrDtlsAuthMissing
	}
	info, err := ParseDtlsAuthInfo(attr)
	if err != nil {
		return nil, err
	}
	algo, digest, err := sdpFingerprint(sdp)
	if err != nil {
		return info, err
	}
	return info, info.Verify(userID, algo, digest, expectedKey)
}

// Verify checks the signature against a fingerprint ("sha-256", "AB:CD:…").
func (a *DtlsAuthInfo) Verify(userID int64, algo, digest string, expectedKey []byte) error {
	pub, err := normalizeIdentityKey(a.PublicKey)
	if err != nil {
		return err
	}
	if expectedKey != nil {
		want, err := normalizeIdentityKey(expectedKey)
		if err != nil {
			return err
		}
		if !bytes.Equal(pub, want) {
			return ErrDtlsAuthKeyMismatch
		}
	}
	if len(a.Signature) != 64 {
		return fmt.Errorf("%w: signature is %d bytes", ErrDtlsAuthSignature, len(a.Signature))
	}
	key, err := ecc.DecodePoint(pub, 0)
	if err != nil {
		return err
	}
	msg := DtlsAuthMessage(userID, a.DeviceID, a.IdentityKeyMode, algo, digest, pub)
	if !ecc.VerifySignature(key, msg, [64]byte(a.Signature)) {
		return ErrDtlsAuthSignature
	}
	return nil
}
