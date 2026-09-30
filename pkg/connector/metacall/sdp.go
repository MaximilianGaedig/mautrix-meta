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
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.

// Package metacall adapts Meta's signalling protocol to the reusable call bridge.
package metacall

import (
	"errors"
	"regexp"
	"strings"

	"maunium.net/go/mautrix/bridgev2/callbridge"

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

const webICEOptions = "a=ice-options:trickle fb-force-5245 renomination"

// PrepareLocalSDP turns a Pion SDP into what the Meta web client sends.
func PrepareLocalSDP(sdp string, id *Identity, video bool) (string, error) {
	sdp = fingerprintLineRe.ReplaceAllStringFunc(sdp, func(line string) string {
		m := fingerprintLineRe.FindStringSubmatch(line)
		return m[1] + strings.ToUpper(m[2]) + m[3]
	})
	sdp = iceOptionsLineRe.ReplaceAllString(sdp, "")
	sdp = icePwdLineRe.ReplaceAllStringFunc(sdp, func(line string) string {
		nl := "\n"
		if strings.HasSuffix(line, "\r\n") {
			nl = "\r\n"
		}
		return line + webICEOptions + nl
	})
	if !video {
		sdp = callbridge.DisableVideoSending(sdp)
	}
	sdp = rtcsignal.StripDtlsAuth(sdp)
	info, err := rtcsignal.SignDTLSAuth(id.Priv, id.Pub[:], id.UserID, id.DeviceID, sdp)
	if err != nil {
		return "", err
	}
	return rtcsignal.AddDtlsAuth(sdp, info.String()), nil
}

// PrepareRemoteSDP removes Meta-only attributes before passing SDP to Pion.
func PrepareRemoteSDP(sdp string) string {
	return markICELite(callbridge.CollapseSimulcast(rtcsignal.StripDtlsAuth(sdp)))
}

// markICELite declares Messenger's side ICE-lite, which makes the bridge the controlling agent
// (RFC 8445 6.1.1: a full agent facing a lite one controls).
//
// Messenger's peers act as the controlled agent whichever side made the offer. When the bridge
// answered a Messenger offer - every incoming call - both ends were controlled: Pion logged "Role
// conflict ... same role(controlled)", nobody nominated a pair, and once Messenger renegotiated
// (a camera turned on or off restarts ICE) the connection lost consent 30 s later and the call
// dropped. Calls the bridge offered, where it was controlling anyway, never did.
func markICELite(sdp string) string {
	if sdp == "" || strings.Contains(sdp, "a=ice-lite") {
		return sdp
	}
	i := strings.Index(sdp, "\nm=")
	if i < 0 {
		return sdp
	}
	eol := "\n"
	if i > 0 && sdp[i-1] == '\r' {
		eol = "\r\n"
		i--
	}
	return sdp[:i] + eol + "a=ice-lite" + sdp[i:]
}

// AnswerDelta converts a Meta SFU delta, applies it to leg, and answers it.
func AnswerDelta(leg *callbridge.Leg, update *rtcsignal.SessionDescriptionUpdate) (string, error) {
	remote := leg.PC.RemoteDescription()
	if remote == nil {
		return "", errors.New("delta update before the first remote description")
	}
	converted := &callbridge.SessionDescriptionUpdate{Media: make(map[int32]callbridge.MediaDescriptionUpdate, len(update.Media))}
	for index, media := range update.Media {
		converted.Media[index] = callbridge.MediaDescriptionUpdate{Body: media.Body, MSID: media.MSID, MID: media.MID}
	}
	offer, err := callbridge.ApplySDPDelta(remote.SDP, converted)
	if err != nil {
		return "", err
	}
	return leg.AnswerRenegotiation(PrepareRemoteSDP(offer))
}
