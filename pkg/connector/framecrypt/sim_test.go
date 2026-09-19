package framecrypt

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
)

// call simulates the SFU side of a group call: it sends every member an
// E2eeServerState listing the other members (as the server does in
// JoinResponse and media updates) and relays E2eeKey data messages between
// members.
type call struct {
	t       testing.TB
	cfg     serverConfig
	members []*party
	cs      map[*party]*rtcsignal.E2eeClientState
	// lastIndex is the last non-empty key index each member's module
	// reported.
	lastIndex map[*party]string
	// updates records every server update result per member.
	updates map[*party][]*ServerUpdateResult
	// relayed counts delivered messages.
	relayed int
	// quiet suppresses per-message logs.
	quiet bool
}

func (c *call) logf(format string, args ...any) {
	if !c.quiet {
		c.t.Logf(format, args...)
	}
}

func newCall(t testing.TB, cfg serverConfig) *call {
	return &call{t: t, cfg: cfg, cs: map[*party]*rtcsignal.E2eeClientState{},
		lastIndex: map[*party]string{}, updates: map[*party][]*ServerUpdateResult{}}
}

func (c *call) join(ps ...*party) {
	c.t.Helper()
	for _, p := range ps {
		c.cs[p] = p.clientState(c.t)
		c.members = append(c.members, p)
	}
	for _, a := range c.members {
		for _, b := range c.members {
			if a != b {
				a.mu.Lock()
				a.peers[b.userID] = b.keyPair.PublicKey().Serialize()
				a.mu.Unlock()
			}
		}
	}
	c.pushState()
}

func (c *call) leave(p *party) {
	c.t.Helper()
	for i, m := range c.members {
		if m == p {
			c.members = append(c.members[:i], c.members[i+1:]...)
			break
		}
	}
	c.pushState()
}

func (c *call) pushState() {
	c.t.Helper()
	for _, p := range c.members {
		eps := map[string]testEndpoint{}
		for _, o := range c.members {
			if o != p {
				cs := c.cs[o]
				eps[o.e2eeID()] = testEndpoint{PreKeyBundle: cs.PreKeyBundle, IdentityKeyMode: cs.IdentityKeyMode, DeviceID: cs.DeviceID}
			}
		}
		res, err := p.km.ProcessE2eeServerUpdate(context.Background(), buildServerStateWith(c.cfg, eps))
		if err != nil {
			c.t.Fatal(err)
		}
		c.logf("%s server update (%d members): err=%d keyIndex=%q delay=%s", p.name, len(c.members), res.ErrorCode, res.KeyIndex, res.KeyUpdateDelay)
		c.updates[p] = append(c.updates[p], res)
		if res.KeyIndex != "" {
			c.lastIndex[p] = res.KeyIndex
		}
	}
}

func (c *call) member(uid string) *party {
	for _, m := range c.members {
		if m.userID == uid {
			return m
		}
	}
	return nil
}

// relay delivers queued messages until no member has anything to send.
// Messages to non-members are dropped, like the SFU drops messages to
// users who left.
func (c *call) relay() int {
	c.t.Helper()
	n := 0
	for round := 0; round < 20; round++ {
		moved := 0
		for _, from := range c.members {
			for _, m := range from.takeOut() {
				uid, _, _ := strings.Cut(m.to, ":")
				to := c.member(uid)
				if to == nil {
					c.logf("%s → %q dropped (not a member)", from.name, redactID(m.to))
					continue
				}
				res, err := to.km.ProcessE2eeMessage(context.Background(), m.data)
				if err != nil {
					c.t.Fatal(err)
				}
				c.logf("%s → %s (%d bytes): status=%d keyIndex=%q delay=%s", from.name, to.name, len(m.data), res.StatusCode, res.KeyIndex, res.KeyUpdateDelay)
				if res.KeyIndex != "" {
					c.lastIndex[to] = res.KeyIndex
				}
				moved++
			}
		}
		n += moved
		if moved == 0 {
			break
		}
	}
	c.relayed += n
	return n
}

// settle relays until nothing new arrived for quiet (timers in the module
// may send more messages).
func (c *call) settle(quiet, max time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(max)
	last := time.Now()
	for time.Now().Before(deadline) {
		if c.relay() > 0 {
			last = time.Now()
		} else if time.Since(last) >= quiet {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
