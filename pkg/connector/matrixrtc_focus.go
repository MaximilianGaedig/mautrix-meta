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

package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/id"
)

// rtcTransport is a MatrixRTC focus (MSC4143): for LiveKit, the lk-jwt-service that hands out tokens.
type rtcTransport struct {
	Type              string `json:"type"`
	LivekitServiceURL string `json:"livekit_service_url"`
}

var rtcHTTP = &http.Client{Timeout: 10 * time.Second}

// discoverRTCTransport finds the homeserver's LiveKit focus: the MSC4143 rtc/transports endpoint,
// then .well-known/matrix/client's org.matrix.msc4143.rtc_foci (what Element X reads).
func discoverRTCTransport(ctx context.Context, cli *mautrix.Client, serverName string) (*rtcTransport, error) {
	var transports struct {
		RTCTransports []rtcTransport `json:"rtc_transports"`
	}
	url := cli.BuildClientURL("unstable", "org.matrix.msc4143", "rtc", "transports")
	if _, err := cli.MakeRequest(ctx, http.MethodGet, url, nil, &transports); err == nil {
		if t := firstLiveKit(transports.RTCTransports); t != nil {
			return t, nil
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+serverName+"/.well-known/matrix/client", nil)
	if err != nil {
		return nil, err
	}
	resp, err := rtcHTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch well-known: %w", err)
	}
	defer resp.Body.Close()
	var wk struct {
		RTCFoci []rtcTransport `json:"org.matrix.msc4143.rtc_foci"`
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&wk); err != nil {
		return nil, fmt.Errorf("parse well-known: %w", err)
	}
	if t := firstLiveKit(wk.RTCFoci); t != nil {
		return t, nil
	}
	return nil, errors.New("the homeserver advertises no LiveKit focus")
}

func firstLiveKit(ts []rtcTransport) *rtcTransport {
	for i := range ts {
		if ts[i].Type == "livekit" && ts[i].LivekitServiceURL != "" {
			return &ts[i]
		}
	}
	return nil
}

// liveKitToken asks the focus's lk-jwt-service for a token to the room's call, as `cli`'s user
// (proven with an OpenID token): the same LiveKit room Element X joins for this Matrix room.
func liveKitToken(ctx context.Context, cli *mautrix.Client, focus *rtcTransport, roomID id.RoomID, deviceID string) (url, token string, err error) {
	oid, err := cli.RequestOpenIDToken(ctx)
	if err != nil {
		return "", "", fmt.Errorf("openid token: %w", err)
	}
	body, err := json.Marshal(map[string]any{
		"room":         roomID,
		"openid_token": oid,
		"device_id":    deviceID,
	})
	if err != nil {
		return "", "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimSuffix(focus.LivekitServiceURL, "/")+"/sfu/get", bytes.NewReader(body))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := rtcHTTP.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("lk-jwt-service: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", "", fmt.Errorf("lk-jwt-service: HTTP %d: %s", resp.StatusCode, msg)
	}
	var out struct {
		URL string `json:"url"`
		JWT string `json:"jwt"`
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&out); err != nil {
		return "", "", fmt.Errorf("parse lk-jwt-service response: %w", err)
	}
	if out.URL == "" || out.JWT == "" {
		return "", "", errors.New("lk-jwt-service returned no url/jwt")
	}
	return out.URL, out.JWT, nil
}
