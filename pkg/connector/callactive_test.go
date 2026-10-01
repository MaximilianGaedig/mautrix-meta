package connector

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func askCallActive(t *testing.T, cbs []*callBridge, remoteAddr string, header ...string) (int, string) {
	t.Helper()
	h := callActiveHandler(func() int {
		n := 0
		for _, cb := range cbs {
			n += cb.calls()
		}
		return n
	})
	r := httptest.NewRequest(http.MethodGet, callActivePath, nil)
	r.RemoteAddr = remoteAddr
	for i := 0; i+1 < len(header); i += 2 {
		r.Header.Set(header[i], header[i+1])
	}
	w := httptest.NewRecorder()
	h(w, r)
	return w.Code, strings.TrimSpace(w.Body.String())
}

// What docker-pre-update.sh reads to put an update off: idle, a 1:1 call, a group call beside it.
func TestCallActive(t *testing.T) {
	idle := &callBridge{log: zerolog.Nop()}
	busy := &callBridge{log: zerolog.Nop()}
	var none *callBridge // a login whose call bridging never started
	cbs := []*callBridge{idle, none, busy}

	code, body := askCallActive(t, cbs, "127.0.0.1:40000")
	if code != http.StatusOK || body != `{"active":false,"calls":0}` {
		t.Fatalf("idle: %d %s", code, body)
	}

	busy.active = &callSession{cb: busy, log: zerolog.Nop()}
	code, body = askCallActive(t, cbs, "127.0.0.1:40000")
	if code != http.StatusOK || body != `{"active":true,"calls":1}` {
		t.Fatalf("1:1 call: %d %s", code, body)
	}

	idle.group = &groupCall{}
	code, body = askCallActive(t, cbs, "[::1]:40000")
	var resp callActiveResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil || code != http.StatusOK || !resp.Active || resp.Calls != 2 {
		t.Fatalf("1:1 and group call: %d %s", code, body)
	}

	busy.active, idle.group = nil, nil
	if _, body = askCallActive(t, cbs, "127.0.0.1:40000"); body != `{"active":false,"calls":0}` {
		t.Fatalf("after the calls ended: %s", body)
	}
}

// The listener is the one the homeserver reaches too; only the bridge's own machine may ask.
func TestCallActiveOnlyFromThisMachine(t *testing.T) {
	busy := &callBridge{log: zerolog.Nop()}
	busy.active = &callSession{cb: busy, log: zerolog.Nop()}
	cbs := []*callBridge{busy}
	for name, ask := range map[string]func() (int, string){
		"another host": func() (int, string) { return askCallActive(t, cbs, "172.18.0.5:40000") },
		"a reverse proxy": func() (int, string) {
			return askCallActive(t, cbs, "127.0.0.1:40000", "X-Forwarded-For", "203.0.113.9")
		},
		"no address": func() (int, string) { return askCallActive(t, cbs, "") },
	} {
		if code, body := ask(); code != http.StatusForbidden || strings.Contains(body, "active") {
			t.Errorf("%s: got %d %s, want 403", name, code, body)
		}
	}
}
