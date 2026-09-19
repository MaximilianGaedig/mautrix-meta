package framecrypt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// mustDecrypt decrypts ct with a fresh audio decryptor of p for sender and
// compares it with want.
func mustDecrypt(t *testing.T, p *party, dec *FrameDecryptor, ct, want []byte, label string) {
	t.Helper()
	pt, err := dec.Decrypt(context.Background(), ct)
	if err != nil {
		t.Fatalf("%s: %s: decrypt: %v", label, p.name, err)
	}
	if !bytes.Equal(pt, want) {
		t.Fatalf("%s: %s: decrypted frame differs", label, p.name)
	}
}

// trailer returns the key index and counter bytes of an encrypted frame
// (the last three bytes are key index, counter, trailer length 3 in every
// frame this module produced in these tests).
func trailer(ct []byte) (keyIndex, counter byte) {
	if len(ct) < 3 || ct[len(ct)-1] != 3 {
		return 0xff, 0xff
	}
	return ct[len(ct)-3], ct[len(ct)-2]
}

// TestKeyLifecycle runs a call through join, mid-call join, leave and the
// rekey that follows, with the sender key index advanced by the keys
// manager's own Go timers (ZenonEncryptionKeysManager.$11 behaviour).
func TestKeyLifecycle(t *testing.T) {
	rt := runtimeForTest(t)
	ctx := context.Background()
	cfg := capturedConfig
	cfg.SenderKeyUpdateDelayMs = 300 // captured: 10000
	c := newCall(t, cfg)
	alice := newPartyOpts(t, rt, "alice", "100000000000001", "aliceCnameAAAAAA", 3, true)
	bob := newPartyOpts(t, rt, "bob", "100000000000002", "bobCnameBBBBBBBB", 7, true)
	carol := newPartyOpts(t, rt, "carol", "100000000000003", "carolCnameCCCCCC", 5, true)
	all := []*party{alice, bob, carol}

	encs := map[*party]*FrameEncryptor{}
	decs := map[[2]*party]*FrameDecryptor{} // [receiver, sender]
	for _, p := range all {
		e, err := p.km.NewFrameEncryptor(ctx)
		if err != nil {
			t.Fatal(err)
		}
		encs[p] = e
		for _, s := range all {
			if s != p {
				d, err := p.km.NewFrameDecryptor(ctx, s.e2eeID(), DecryptorHandlers(true))
				if err != nil {
					t.Fatal(err)
				}
				decs[[2]*party{p, s}] = d
			}
		}
	}
	n := 0
	frame := func(p *party) []byte {
		n++
		return []byte(fmt.Sprintf("opus-frame-%s-%06d-padding-padding", p.name, n))
	}
	enc := func(p *party) ([]byte, []byte) {
		pt := frame(p)
		ct, err := encs[p].Encrypt(ctx, HandlerGeneric, pt)
		if err != nil {
			t.Fatalf("%s encrypt: %v", p.name, err)
		}
		return pt, ct
	}
	checkAll := func(label string, members []*party) {
		for _, s := range members {
			pt, ct := enc(s)
			for _, r := range members {
				if r != s {
					mustDecrypt(t, r, decs[[2]*party{r, s}], ct, pt, label)
				}
			}
		}
	}

	// 1. alice and bob.
	c.join(alice, bob)
	c.settle(400*time.Millisecond, 5*time.Second)
	checkAll("alice+bob", []*party{alice, bob})
	preJoinPT, preJoinCT := enc(alice)
	ki0, _ := trailer(preJoinCT)

	// 2. carol joins mid-call: keys are exchanged, everyone decrypts
	// everyone. The module ratchets alice's key forward on the join
	// (key index advances without a new key message round).
	c.join(carol)
	c.settle(400*time.Millisecond, 5*time.Second)
	checkAll("after carol joined", all)
	postJoinPT, postJoinCT := enc(alice)
	ki1, _ := trailer(postJoinCT)
	t.Logf("alice key index before carol joined %d, after %d", ki0, ki1)
	if ki1 == ki0 {
		t.Errorf("key index did not advance on join (%d)", ki0)
	}
	// Carol must not read frames from before she joined.
	if _, err := decs[[2]*party{carol, alice}].Decrypt(ctx, preJoinCT); err == nil {
		t.Fatal("carol decrypted a frame alice sent before carol joined")
	} else {
		t.Logf("carol, pre-join frame: %v", err)
	}
	// Bob still decrypts the older frame: the previous index is within
	// ratchetSpace (8).
	mustDecrypt(t, bob, decs[[2]*party{bob, alice}], preJoinCT, preJoinPT, "pre-join frame at bob")

	// 3. carol leaves. The server update carries a new key index and the
	// keys managers switch to it after keyUpdateDelay, via their timers.
	c.leave(carol)
	for _, p := range []*party{alice, bob} {
		u := c.updates[p][len(c.updates[p])-1]
		if u.KeyIndex == "" || u.KeyIndex == "0" || u.KeyUpdateDelay != 300*time.Millisecond {
			t.Fatalf("%s: leave update: keyIndex=%q delay=%s, want a new index after 300ms", p.name, u.KeyIndex, u.KeyUpdateDelay)
		}
	}
	// Until the delay expires the old key is still used, so frames sent
	// in that window remain readable for everyone who had it.
	windowPT, windowCT := enc(alice)
	if ki, _ := trailer(windowCT); ki != ki1 {
		t.Errorf("key index changed before keyUpdateDelay: %d → %d", ki1, ki)
	}
	c.settle(500*time.Millisecond, 5*time.Second) // delivers the new keys; timers fire
	newPT, newCT := enc(alice)
	ki2, _ := trailer(newCT)
	t.Logf("alice key index after carol left and the 300ms timer fired: %d", ki2)
	if ki2 == ki1 {
		t.Fatalf("sender key not rotated after the leave (index %d)", ki1)
	}
	mustDecrypt(t, bob, decs[[2]*party{bob, alice}], newCT, newPT, "after rekey")
	pt, ct := enc(bob)
	mustDecrypt(t, alice, decs[[2]*party{alice, bob}], ct, pt, "after rekey (bob→alice)")
	for _, s := range []*party{alice, bob} {
		pt, ct := enc(s)
		_, err := decs[[2]*party{carol, s}].Decrypt(ctx, ct)
		if !errors.Is(err, ErrMissingKey) {
			t.Fatalf("carol after leaving, frame from %s: %v (%d bytes), want ERROR_MISSING_KEY", s.name, err, len(pt))
		}
		t.Logf("carol after leaving, frame from %s: %v", s.name, err)
	}
	// Frames from before the rotation still decrypt at bob.
	mustDecrypt(t, bob, decs[[2]*party{bob, alice}], windowCT, windowPT, "pre-rotation frame after rotation")
	mustDecrypt(t, bob, decs[[2]*party{bob, alice}], postJoinCT, postJoinPT, "older frame after rotation")
	// Replays are rejected.
	if _, err := decs[[2]*party{bob, alice}].Decrypt(ctx, newCT); !errors.Is(err, ErrSeenFrame) && !errors.Is(err, ErrFrameTooOld) {
		t.Errorf("replayed frame: %v, want ERROR_SEEN_FRAME", err)
	} else {
		t.Logf("replayed frame: %v", err)
	}
}

// TestMediaChannelRetryTimers exercises the module's own timers
// (startTimer/stopTimer callbacks → wasmTimerAdapter_execute on Go
// timers): with the media data channel up, key messages are also sent over
// SCTP and retried every keyOverMediaDataChannelRetryDelayMs until acked.
func TestMediaChannelRetryTimers(t *testing.T) {
	rt := runtimeForTest(t)
	ctx := context.Background()
	cfg := capturedConfig
	cfg.KeyOverMediaDataChannelRetryDelayMs = 50 // captured: 500
	c := newCall(t, cfg)
	alice := newParty(t, rt, "alice", "100000000000001", "aliceCnameAAAAAA", 3)
	bob := newParty(t, rt, "bob", "100000000000002", "bobCnameBBBBBBBB", 7)
	for _, p := range []*party{alice, bob} {
		if err := p.km.OnMediaDataChannelReady(ctx); err != nil {
			t.Fatal(err)
		}
	}
	c.join(alice, bob)
	// No relaying yet: nobody acks, so the SCTP sends are retried.
	time.Sleep(600 * time.Millisecond)
	stats := func(p *party) (started int32, fired, cancelled, pending, sctp int) {
		p.in.mu.Lock()
		started, fired, cancelled, pending = p.in.timerSeq, p.in.timersFired, p.in.timersCancel, len(p.in.timers)
		p.in.mu.Unlock()
		p.mu.Lock()
		sctp = len(p.sctpOut)
		p.mu.Unlock()
		return
	}
	for _, p := range []*party{alice, bob} {
		s, f, cl, pe, sc := stats(p)
		t.Logf("%s before acks: timers started=%d fired=%d cancelled=%d pending=%d, sctp sends=%d", p.name, s, f, cl, pe, sc)
		if f == 0 || sc < 2 {
			t.Fatalf("%s: no retry timer fired (fired=%d, sctp sends=%d)", p.name, f, sc)
		}
	}
	c.settle(300*time.Millisecond, 5*time.Second)
	time.Sleep(300 * time.Millisecond)
	for _, p := range []*party{alice, bob} {
		s, f, cl, pe, sc := stats(p)
		t.Logf("%s after acks: timers started=%d fired=%d cancelled=%d pending=%d, sctp sends=%d", p.name, s, f, cl, pe, sc)
		if pe != 0 {
			t.Errorf("%s: %d timers still pending after the keys were acked", p.name, pe)
		}
	}
	// And the keys work.
	enc, _ := alice.km.NewFrameEncryptor(ctx)
	dec, _ := bob.km.NewFrameDecryptor(ctx, alice.e2eeID(), DecryptorHandlers(true))
	ct, err := enc.Encrypt(ctx, HandlerGeneric, []byte("hello over sfu"))
	if err != nil {
		t.Fatal(err)
	}
	mustDecrypt(t, bob, dec, ct, []byte("hello over sfu"), "after retries")
}
