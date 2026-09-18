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
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"go.mau.fi/libsignal/ecc"
)

// How E2EE works for Messenger web calls (see messenger-call-signalling.md):
//
//   - 1:1 calls in E2EE threads run with mediaPath P2P. Media is ordinary
//     DTLS-SRTP between the two peers; the server never sees keys. The DTLS
//     certificate fingerprint of each side is bound to the device's Signal
//     identity key by an "a=x-dtls-auth:" SDP attribute (DtlsAuthInfo below),
//     which the peer verifies against the identity key it knows for that
//     user/device. When that succeeds the insertable-streams frame
//     encryption is skipped (ZenonE2ee.canSkipFrameEncryption), so the RTP
//     payload is plain Opus inside SRTP.
//   - Group/SFU calls instead use per-frame SFrame encryption with sender
//     keys exchanged as "E2eeKey" data messages, bootstrapped from the
//     preKeyBundle every client publishes in its E2eeClientState.

// VersionRange is an inclusive E2EE protocol version range.
type VersionRange struct {
	Min, Max int16
}

// E2eeClientState is the "E2eeState" state-sync value a client sends in its
// JOIN (Thrift compact).
type E2eeClientState struct {
	PreKeyBundle       []byte         // 1 (see ParsePreKeyBundle)
	CipherSuites       []int16        // 2
	SupportedVersions  []VersionRange // 3.1
	KeyNegotiationMode int16          // 4.1
	AllowOptionalGfd   bool           // 4.3
	IdentityKeyMode    int16          // 5.1
	DeviceID           int32          // 6 (Signal device id of this browser)
	KeyNegotiationProt []int32        // 8
}

func decodeVersionRanges(r *reader) ([]VersionRange, error) {
	var out []VersionRange
	err := r.readStruct(func(id int16, t Type) error {
		if id != 1 || t != TypeList {
			return r.skip(t)
		}
		return r.structList(func() error {
			var vr VersionRange
			err := r.readStruct(func(id int16, t Type) (err error) {
				switch {
				case id == 1 && t == TypeI16:
					vr.Min, err = r.i16()
				case id == 2 && t == TypeI16:
					vr.Max, err = r.i16()
				default:
					err = r.skip(t)
				}
				return
			})
			out = append(out, vr)
			return err
		})
	})
	return out, err
}

func (w *writer) versionRanges(id int16, v []VersionRange) {
	w.fieldStruct(id, func() {
		w.fieldHeader(1, TypeList)
		w.listHeader(TypeStruct, len(v))
		for _, vr := range v {
			w.structBegin()
			w.fieldI16(1, vr.Min)
			w.fieldI16(2, vr.Max)
			w.structEnd()
		}
	})
}

func decodeIdentityKeyMode(r *reader) (mode int16, err error) {
	err = r.readStruct(func(id int16, t Type) (err error) {
		if id == 1 && t == TypeI16 {
			mode, err = r.i16()
			return
		}
		return r.skip(t)
	})
	return
}

// ParseE2eeClientState decodes an E2eeClientState.
func ParseE2eeClientState(data []byte) (*E2eeClientState, error) {
	s := &E2eeClientState{}
	r := newReader(data)
	err := r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeBinary:
			s.PreKeyBundle, err = r.binary()
		case id == 2 && t == TypeList:
			s.CipherSuites, err = r.i16List()
		case id == 3 && t == TypeStruct:
			s.SupportedVersions, err = decodeVersionRanges(r)
		case id == 4 && t == TypeStruct:
			err = r.readStruct(func(id int16, t Type) (err error) {
				switch {
				case id == 1 && t == TypeI16:
					s.KeyNegotiationMode, err = r.i16()
				case id == 3 && (t == TypeTrue || t == TypeFalse):
					s.AllowOptionalGfd = t == TypeTrue
				default:
					err = r.skip(t)
				}
				return
			})
		case id == 5 && t == TypeStruct:
			s.IdentityKeyMode, err = decodeIdentityKeyMode(r)
		case id == 6 && t == TypeI32:
			s.DeviceID, err = r.i32()
		case id == 8 && t == TypeList:
			s.KeyNegotiationProt, err = r.i32List()
		default:
			err = r.skip(t)
		}
		return
	})
	if err != nil {
		return nil, fmt.Errorf("rtcsignal: E2eeClientState: %w", err)
	}
	return s, nil
}

// Marshal encodes the client state in the field order the web client uses.
func (s *E2eeClientState) Marshal() []byte {
	w := &writer{}
	w.structBegin()
	w.fieldBinary(1, s.PreKeyBundle)
	w.fieldI16List(2, s.CipherSuites)
	w.versionRanges(3, s.SupportedVersions)
	w.fieldStruct(4, func() {
		w.fieldI16(1, s.KeyNegotiationMode)
		w.fieldBool(3, s.AllowOptionalGfd)
	})
	w.fieldStruct(5, func() { w.fieldI16(1, s.IdentityKeyMode) })
	w.fieldI32(6, s.DeviceID)
	w.fieldHeader(7, TypeMap)
	w.mapHeader(0, 0, 0)
	w.fieldHeader(8, TypeList)
	w.listHeader(TypeI32, len(s.KeyNegotiationProt))
	for _, v := range s.KeyNegotiationProt {
		w.zigzag(int64(v))
	}
	w.structEnd()
	return w.bytes()
}

// E2eeEndpointInfo is a remote endpoint's key material in E2eeServerState.
type E2eeEndpointInfo struct {
	PreKeyBundle    []byte // 1
	IdentityKeyMode int16  // 2.1
	DeviceID        int32  // 3
}

// E2eeServerState is the server's "E2eeState" value. In the captured P2P
// call it was empty apart from a default config: the server does not
// distribute key material for P2P calls, the x-dtls-auth attribute does.
type E2eeServerState struct {
	CipherSuites       []int16                     // 2
	SupportedVersions  []VersionRange              // 3.1
	KeyNegotiationMode int16                       // 4.1
	EndpointInfos      map[string]E2eeEndpointInfo // 5, key "<userId>:<...>"
	NegotiatedProtocol int32                       // 7
}

// ParseE2eeServerState decodes an E2eeServerState.
func ParseE2eeServerState(data []byte) (*E2eeServerState, error) {
	s := &E2eeServerState{EndpointInfos: map[string]E2eeEndpointInfo{}}
	r := newReader(data)
	err := r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 2 && t == TypeList:
			s.CipherSuites, err = r.i16List()
		case id == 3 && t == TypeStruct:
			s.SupportedVersions, err = decodeVersionRanges(r)
		case id == 4 && t == TypeStruct:
			err = r.readStruct(func(id int16, t Type) (err error) {
				if id == 1 && t == TypeI16 {
					s.KeyNegotiationMode, err = r.i16()
					return
				}
				return r.skip(t)
			})
		case id == 5 && t == TypeMap:
			err = r.stringMap(func(k string, vt Type) error {
				if vt != TypeStruct {
					return r.skip(vt)
				}
				var ei E2eeEndpointInfo
				err := r.readStruct(func(id int16, t Type) (err error) {
					switch {
					case id == 1 && t == TypeBinary:
						ei.PreKeyBundle, err = r.binary()
					case id == 2 && t == TypeStruct:
						ei.IdentityKeyMode, err = decodeIdentityKeyMode(r)
					case id == 3 && t == TypeI32:
						ei.DeviceID, err = r.i32()
					default:
						err = r.skip(t)
					}
					return
				})
				s.EndpointInfos[k] = ei
				return err
			})
		case id == 7 && t == TypeI32:
			s.NegotiatedProtocol, err = r.i32()
		default:
			err = r.skip(t)
		}
		return
	})
	if err != nil {
		return nil, fmt.Errorf("rtcsignal: E2eeServerState: %w", err)
	}
	return s, nil
}

// PreKeyBundle is the Signal-style bundle inside E2eeClientState.preKeyBundle.
// Its Thrift schema lives inside the frame-encryption WASM, so the field
// names here are inferred: field 4 is the 33-byte identity key (the same key
// that signs x-dtls-auth), field 5 a signed pre-key whose signature verifies
// with that identity key, field 6 a one-time pre-key.
type PreKeyBundle struct {
	IdentityKey     []byte // 4 (0x05 || curve25519)
	SignedPreKeyID  int32  // 5.2.3
	SignedPreKey    []byte // 5.2.2
	SignedPreKeySig []byte // 5.3
	PreKeyID        int32  // 6.3
	PreKey          []byte // 6.2
}

func decodeKeyWithID(r *reader) (key []byte, id int32, err error) {
	err = r.readStruct(func(fid int16, t Type) (err error) {
		switch {
		case fid == 2 && t == TypeBinary:
			key, err = r.binary()
		case fid == 3 && t == TypeI32:
			id, err = r.i32()
		default:
			err = r.skip(t)
		}
		return
	})
	return
}

// ParsePreKeyBundle decodes the bundle.
func ParsePreKeyBundle(data []byte) (*PreKeyBundle, error) {
	b := &PreKeyBundle{}
	r := newReader(data)
	err := r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 4 && t == TypeBinary:
			b.IdentityKey, err = r.binary()
		case id == 5 && t == TypeStruct:
			err = r.readStruct(func(id int16, t Type) (err error) {
				switch {
				case id == 2 && t == TypeStruct:
					b.SignedPreKey, b.SignedPreKeyID, err = decodeKeyWithID(r)
				case id == 3 && t == TypeBinary:
					b.SignedPreKeySig, err = r.binary()
				default:
					err = r.skip(t)
				}
				return
			})
		case id == 6 && t == TypeStruct:
			b.PreKey, b.PreKeyID, err = decodeKeyWithID(r)
		default:
			err = r.skip(t)
		}
		return
	})
	if err != nil {
		return nil, fmt.Errorf("rtcsignal: preKeyBundle: %w", err)
	}
	return b, nil
}

// Marshal encodes the bundle with the known fields only (the web client also
// sends zero-valued fields 2, 3 and 12, whose meaning is unknown).
func (b *PreKeyBundle) Marshal() []byte {
	w := &writer{}
	w.structBegin()
	w.fieldBinary(4, b.IdentityKey)
	w.fieldStruct(5, func() {
		w.fieldStruct(2, func() {
			w.fieldBinary(2, b.SignedPreKey)
			w.fieldI32(3, b.SignedPreKeyID)
		})
		w.fieldBinary(3, b.SignedPreKeySig)
	})
	w.fieldStruct(6, func() {
		w.fieldBinary(2, b.PreKey)
		w.fieldI32(3, b.PreKeyID)
	})
	w.structEnd()
	return w.bytes()
}

var errBadKey = errors.New("rtcsignal: key is not a 33-byte curve25519 point")

// VerifySignedPreKey checks the signed pre-key signature with the identity
// key (XEdDSA, as in Signal).
func (b *PreKeyBundle) VerifySignedPreKey() (bool, error) {
	if len(b.IdentityKey) != 33 || len(b.SignedPreKey) != 33 || len(b.SignedPreKeySig) != 64 {
		return false, errBadKey
	}
	ik, err := ecc.DecodePoint(b.IdentityKey, 0)
	if err != nil {
		return false, err
	}
	return ecc.VerifySignature(ik, b.SignedPreKey, [64]byte(b.SignedPreKeySig)), nil
}

// DtlsAuthInfo is the DtlsAuthenticationInfo carried base64 (standard, with
// padding) in the "a=x-dtls-auth:" SDP session attribute. The signature is an
// XEdDSA signature by the identity key over a message derived from the DTLS
// fingerprint; the exact message layout is computed inside the WASM
// (context string "dtls_authentication_context") and is not known yet.
type DtlsAuthInfo struct {
	ProtocolVersion int32  // 1 (1 in the capture)
	IdentityKeyMode int16  // 2.1 (2 in the capture)
	DeviceID        int32  // 2.2, Signal device id
	PublicKey       []byte // 2.3, 33-byte identity key
	Signature       []byte // 3, 64 bytes
}

// ParseDtlsAuthInfo decodes the attribute value (without "a=x-dtls-auth:").
func ParseDtlsAuthInfo(attr string) (*DtlsAuthInfo, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(attr))
	if err != nil {
		return nil, fmt.Errorf("rtcsignal: x-dtls-auth: %w", err)
	}
	a := &DtlsAuthInfo{}
	r := newReader(raw)
	err = r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeI32:
			a.ProtocolVersion, err = r.i32()
		case id == 2 && t == TypeStruct:
			err = r.readStruct(func(id int16, t Type) (err error) {
				switch {
				case id == 1 && t == TypeI16:
					a.IdentityKeyMode, err = r.i16()
				case id == 2 && t == TypeI32:
					a.DeviceID, err = r.i32()
				case id == 3 && t == TypeBinary:
					a.PublicKey, err = r.binary()
				default:
					err = r.skip(t)
				}
				return
			})
		case id == 3 && t == TypeBinary:
			a.Signature, err = r.binary()
		default:
			err = r.skip(t)
		}
		return
	})
	if err != nil {
		return nil, fmt.Errorf("rtcsignal: x-dtls-auth: %w", err)
	}
	return a, nil
}

// String encodes the info as an attribute value.
func (a *DtlsAuthInfo) String() string {
	w := &writer{}
	w.structBegin()
	w.fieldI32(1, a.ProtocolVersion)
	w.fieldStruct(2, func() {
		w.fieldI16(1, a.IdentityKeyMode)
		w.fieldI32(2, a.DeviceID)
		w.fieldBinary(3, a.PublicKey)
	})
	w.fieldBinary(3, a.Signature)
	w.structEnd()
	return base64.StdEncoding.EncodeToString(w.bytes())
}

var (
	dtlsAuthRe    = regexp.MustCompile(`(?m)^a=x-dtls-auth:(\S+)\r?(?:\n|$)`)
	fingerprintRe = regexp.MustCompile(`(?m)^a=fingerprint:(\S+) (\S+)\r?$`)
)

// SDPDtlsAuth returns the x-dtls-auth attribute of an SDP, or "" if absent.
// The web client strips it before handing the SDP to RTCPeerConnection and
// adds it back to the local SDP it signals, which is why
// chrome://webrtc-internals never shows it.
func SDPDtlsAuth(sdp string) string {
	if m := dtlsAuthRe.FindStringSubmatch(sdp); m != nil {
		return m[1]
	}
	return ""
}

// StripDtlsAuth removes the x-dtls-auth attribute (for feeding a remote SDP
// to a WebRTC stack).
func StripDtlsAuth(sdp string) string { return dtlsAuthRe.ReplaceAllString(sdp, "") }

// AddDtlsAuth inserts an x-dtls-auth session attribute after the
// "a=msid-semantic" or "a=group" line, mirroring the web client.
func AddDtlsAuth(sdp, attr string) string {
	lines := strings.SplitAfter(sdp, "\n")
	insertAt := -1
	for i, l := range lines {
		if strings.HasPrefix(l, "m=") {
			break
		}
		if strings.HasPrefix(l, "a=group:") || strings.HasPrefix(l, "a=msid-semantic") {
			insertAt = i + 1
		}
	}
	if insertAt < 0 {
		return sdp
	}
	nl := "\r\n"
	if !strings.HasSuffix(lines[insertAt-1], "\r\n") {
		nl = "\n"
	}
	out := append([]string{}, lines[:insertAt]...)
	out = append(out, "a=x-dtls-auth:"+attr+nl)
	return strings.Join(append(out, lines[insertAt:]...), "")
}

// SDPFingerprints returns the distinct "algo digest" DTLS fingerprints of an
// SDP. The web client refuses to sign or verify unless there is exactly one.
func SDPFingerprints(sdp string) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range fingerprintRe.FindAllStringSubmatch(sdp, -1) {
		fp := m[1] + " " + m[2]
		if !seen[fp] {
			seen[fp] = true
			out = append(out, fp)
		}
	}
	return out
}

// ServerInfoData is the decoded form of the opaque call handle that appears
// as header.serverInfoData, in the fb:call notification's server_info_data
// attribute, in the call window URL and in joining_context JSON. It is a
// base64 Thrift compact struct.
type ServerInfoData struct {
	Region  string // 1 (e.g. a short datacenter/cluster tag)
	CallKey string // 3 (16 chars)
	Number  int64  // 4 (meaning unknown)
}

// ParseServerInfoData decodes a server_info_data string.
func ParseServerInfoData(s string) (*ServerInfoData, error) {
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		if raw, err = base64.RawURLEncoding.DecodeString(s); err != nil {
			return nil, fmt.Errorf("rtcsignal: server_info_data: %w", err)
		}
	}
	d := &ServerInfoData{}
	r := newReader(raw)
	err = r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeBinary:
			d.Region, err = r.string()
		case id == 3 && t == TypeBinary:
			d.CallKey, err = r.string()
		case id == 4 && t == TypeI64:
			d.Number, err = r.i64()
		default:
			err = r.skip(t)
		}
		return
	})
	if err != nil {
		return nil, fmt.Errorf("rtcsignal: server_info_data: %w", err)
	}
	return d, nil
}

// String re-encodes the handle.
func (d *ServerInfoData) String() string {
	w := &writer{}
	w.structBegin()
	w.fieldString(1, d.Region)
	w.fieldString(3, d.CallKey)
	w.fieldI64(4, d.Number)
	w.structEnd()
	return base64.StdEncoding.EncodeToString(w.bytes())
}

// EndpointUserID extracts the user id from an E2eeServerState endpointInfos
// key ("<userId>:<...>"), as ZenonE2eeCore does.
func EndpointUserID(key string) (int64, bool) {
	uid, _, _ := strings.Cut(key, ":")
	v, err := strconv.ParseInt(uid, 10, 64)
	return v, err == nil
}
