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
	"encoding/json"
	"net"
	"net/http"

	"maunium.net/go/mautrix/bridgev2/matrix"
)

// callActivePath answers "is a call in progress?" on the appservice listener, beside the library's
// /_matrix/mau/live and /_matrix/mau/ready. A restart of the bridge drops every call, and every
// push to the fork makes watchtower restart it: docker-pre-update.sh asks here first and puts the
// update off while someone is talking.
const callActivePath = "/_matrix/mau/call_active"

type callActiveResponse struct {
	Active bool `json:"active"`
	Calls  int  `json:"calls"`
}

// registerCallActiveRoute adds callActivePath to the appservice's own router. Not through
// GetRouter(), which is only there with a public address set: this route is for the bridge's own
// machine and needs none.
func (m *MetaConnector) registerCallActiveRoute() {
	mx, ok := m.Bridge.Matrix.(*matrix.Connector)
	if !ok || mx.AS == nil || mx.AS.Router == nil {
		return
	}
	mx.AS.Router.HandleFunc("GET "+callActivePath, callActiveHandler(m.activeCalls))
}

// activeCalls counts the bridged calls of every login: ringing, connecting or connected.
func (m *MetaConnector) activeCalls() int {
	n := 0
	for _, login := range m.Bridge.GetAllCachedUserLogins() {
		if client, ok := login.Client.(*MetaClient); ok && client != nil {
			n += client.callBridge.Load().calls()
		}
	}
	return n
}

// calls is how many calls this login has now: its 1:1 call and its group call.
func (cb *callBridge) calls() int {
	if cb == nil {
		return 0
	}
	cb.lock.Lock()
	defer cb.lock.Unlock()
	n := 0
	if cb.active != nil {
		n++
	}
	if cb.group != nil {
		n++
	}
	return n
}

// callActiveHandler serves callActivePath. It has no token, like the health checks next to it, and
// instead answers only the machine (or container) the bridge runs on: the listener is also what
// the homeserver talks to, and whether someone is on a call is nobody else's business.
func callActiveHandler(calls func() int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !fromThisMachine(r) {
			http.Error(w, "only available from the bridge's own machine", http.StatusForbidden)
			return
		}
		n := calls()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(callActiveResponse{Active: n > 0, Calls: n})
	}
}

// fromThisMachine reports whether a request came over loopback and not through a reverse proxy,
// whose requests come over loopback too but carry someone else's.
func fromThisMachine(r *http.Request) bool {
	if r.Header.Get("X-Forwarded-For") != "" || r.Header.Get("Forwarded") != "" {
		return false
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
