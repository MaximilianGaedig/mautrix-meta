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

package callbridge

import (
	"regexp"
	"strings"

	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
)

// Identity is the Signal identity of the bridge's E2EE device, which signs
// the DTLS fingerprint (x-dtls-auth) of every SDP sent to Messenger.
type Identity struct {
	UserID   int64
	DeviceID int32
	Priv     [32]byte
	Pub      [32]byte
}

var (
	fingerprintLineRe = regexp.MustCompile(`(?m)^(a=fingerprint:\S+ )([0-9A-Fa-f:]+)(\r?)$`)
	iceOptionsLineRe  = regexp.MustCompile(`(?m)^a=ice-options:[^\r\n]*\r?\n`)
	icePwdLineRe      = regexp.MustCompile(`(?m)^a=ice-pwd:[^\r\n]*\r?\n`)
)

// webICEOptions is the ice-options attribute of the web client.
const webICEOptions = "a=ice-options:trickle fb-force-5245 renomination"

// PrepareMetaLocalSDP turns a Pion SDP into what the web client would send:
// fingerprints upper-case like Chrome (the signed digest is the SDP text,
// and fingerprint comparison is case-insensitive per RFC 8122), the web
// client's ice-options, and an x-dtls-auth attribute signed with the
// device's identity key.
func PrepareMetaLocalSDP(sdp string, id *Identity) (string, error) {
	sdp = fingerprintLineRe.ReplaceAllStringFunc(sdp, func(line string) string {
		m := fingerprintLineRe.FindStringSubmatch(line)
		return m[1] + strings.ToUpper(m[2]) + m[3]
	})
	// Pion writes no ice-options; the web client has one per m-section,
	// right after the ICE credentials.
	sdp = iceOptionsLineRe.ReplaceAllString(sdp, "")
	sdp = icePwdLineRe.ReplaceAllStringFunc(sdp, func(line string) string {
		nl := "\n"
		if strings.HasSuffix(line, "\r\n") {
			nl = "\r\n"
		}
		return line + webICEOptions + nl
	})
	sdp = noVideoSending(sdp)
	sdp = rtcsignal.StripDtlsAuth(sdp)
	info, err := rtcsignal.SignDTLSAuth(id.Priv, id.Pub[:], id.UserID, id.DeviceID, sdp)
	if err != nil {
		return "", err
	}
	return rtcsignal.AddDtlsAuth(sdp, info.String()), nil
}

// PrepareMetaRemoteSDP removes the attributes a WebRTC stack must not see,
// like the web client does before setRemoteDescription.
func PrepareMetaRemoteSDP(sdp string) string {
	return rtcsignal.StripDtlsAuth(sdp)
}

// AudioTrackID returns the msid track id of the first audio m-line, which is
// what the web client uses as the key of JoinRequest.mediaStatus.
func AudioTrackID(sdp string) string {
	inAudio := false
	for _, line := range strings.Split(sdp, "\n") {
		line = strings.TrimRight(line, "\r")
		switch {
		case strings.HasPrefix(line, "m="):
			if inAudio {
				return ""
			}
			inAudio = strings.HasPrefix(line, "m=audio")
		case inAudio && strings.HasPrefix(line, "a=msid:"):
			parts := strings.Fields(strings.TrimPrefix(line, "a=msid:"))
			if len(parts) == 2 {
				return parts[1]
			}
		}
	}
	return ""
}

// noVideoSending rewrites the direction of video m-lines so the SDP never
// claims to send video: the bridge has no video track, but Pion answers a
// recvonly video offer with sendonly once video codecs are registered. The
// web client itself answers inactive.
func noVideoSending(sdp string) string {
	lines := strings.SplitAfter(sdp, "\n")
	inVideo := false
	for i, line := range lines {
		trimmed := strings.TrimRight(line, "\r\n")
		nl := line[len(trimmed):]
		switch {
		case strings.HasPrefix(trimmed, "m="):
			inVideo = strings.HasPrefix(trimmed, "m=video")
		case inVideo && trimmed == "a=sendonly":
			lines[i] = "a=inactive" + nl
		case inVideo && trimmed == "a=sendrecv":
			lines[i] = "a=recvonly" + nl
		}
	}
	return strings.Join(lines, "")
}
