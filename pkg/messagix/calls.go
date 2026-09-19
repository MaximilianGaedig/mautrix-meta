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

package messagix

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/google/go-querystring/query"
	"github.com/rs/zerolog"

	"go.mau.fi/mautrix-meta/pkg/messagix/callsignal"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	"go.mau.fi/mautrix-meta/pkg/messagix/useragent"
)

var ErrCallsUnsupported = errors.New("call signalling is not supported on this platform")

// NewCallSignalClient creates (but does not start) the rpsignaling +
// /t_rtc_multi client for this login. deviceID must be stable per login.
func (c *Client) NewCallSignalClient(deviceID string, handler callsignal.Handler, log zerolog.Logger) (*callsignal.Client, error) {
	if c == nil {
		return nil, ErrClientIsNil
	} else if !c.Platform.IsMessenger() || c.Platform == types.FacebookTor {
		return nil, ErrCallsUnsupported
	} else if _, ok := c.endpoints["dgw_rpsignaling"]; !ok {
		return nil, ErrCallsUnsupported
	} else if !c.IsAuthenticated() {
		return nil, fmt.Errorf("messagix-client: not yet authenticated")
	}
	return callsignal.New(callsignal.Options{
		GetCookies:     func() string { return c.GetCookies().String() },
		Origin:         c.GetEndpoint("base_url"),
		RPSignalingURL: c.GetEndpoint("dgw_rpsignaling"),
		MQTTURL:        c.GetEndpoint("edge_chat_mqtt"),
		MQTTRegion:     c.configs.BrowserConfigTable.MessengerWebRegion.Region,
		DialOpts:       *c.http.GetWebsocketDialer(),
		AppID:          c.socket.AppID,
		UserID:         c.socket.UserID,
		DeviceID:       deviceID,
		UserAgent:      useragent.UserAgent,
		Log:            log,
		Handler:        handler,
	}), nil
}

// TURNServer is the relay /videocall/turndiscovery/ hands out to the web
// client for a call. The credentials are secrets: never log them.
type TURNServer struct {
	IP         string `json:"ip"`
	IPv6       string `json:"ip_6"`
	UDPPort    string `json:"udp_port"`
	TCPPort    string `json:"tcp_port"`
	SSLTCPPort string `json:"ssl_tcp_port"`
	TLSPort    string `json:"tls_port"`
	Username   string `json:"username"`
	Password   string `json:"password"`
}

// URLs returns ICE server URLs in the order the web client used them
// (UDP first, then TCP on the ssl_tcp port).
func (t *TURNServer) URLs() []string {
	var urls []string
	if t.IP != "" && t.UDPPort != "" {
		urls = append(urls, "turn:"+t.IP+":"+t.UDPPort+"?transport=udp")
	}
	if t.IP != "" && t.SSLTCPPort != "" {
		urls = append(urls, "turn:"+t.IP+":"+t.SSLTCPPort+"?transport=tcp")
	} else if t.IP != "" && t.TCPPort != "" {
		urls = append(urls, "turn:"+t.IP+":"+t.TCPPort+"?transport=tcp")
	}
	return urls
}

// MessengerSTUNServer is the STUN server the web client's PeerConnection used.
const MessengerSTUNServer = "stun:stun.fbsbx.com:3478"

// FetchTURNServer calls POST /videocall/turndiscovery/?call_id=ZenonPlatform&version=1
// like the web client does before every call.
func (c *Client) FetchTURNServer(ctx context.Context) (*TURNServer, error) {
	if c == nil {
		return nil, ErrClientIsNil
	}
	endpoint, ok := c.endpoints["turndiscovery"]
	if !ok {
		return nil, ErrCallsUnsupported
	}
	form, err := query.Values(c.http.NewHTTPQuery())
	if err != nil {
		return nil, err
	}
	headers := c.http.BuildHeaders(true, false)
	headers.Set("accept", "*/*")
	headers.Set("origin", c.GetEndpoint("base_url"))
	headers.Set("referer", c.GetEndpoint("base_url")+"/")
	headers.Set("sec-fetch-dest", "empty")
	headers.Set("sec-fetch-mode", "cors")
	headers.Set("sec-fetch-site", "same-origin")
	url := endpoint + "?call_id=ZenonPlatform&version=1"
	_, body, err := c.http.MakeRequest(ctx, url, "POST", headers, []byte(form.Encode()), types.FORM)
	if err != nil {
		return nil, fmt.Errorf("turndiscovery request failed: %w", err)
	}
	body = bytes.TrimPrefix(body, httpclient.AntiJSPrefix)
	var resp struct {
		Payload *TURNServer `json:"payload"`
		Error   int         `json:"error"`
	}
	if err = json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse turndiscovery response (%d bytes): %w", len(body), err)
	} else if resp.Payload == nil {
		return nil, fmt.Errorf("turndiscovery returned no payload (error %s)", strconv.Itoa(resp.Error))
	}
	return resp.Payload, nil
}
