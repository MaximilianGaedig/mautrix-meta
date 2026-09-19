package framecrypt

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentTracks drives one encryptor/decryptor pair per track from
// its own goroutine (as the bridge will, one per media track) while
// participants join and leave, so key messages, server updates and the
// key-index timers run concurrently with frame traffic. Run it with -race
// where the race detector is supported.
func TestConcurrentTracks(t *testing.T) {
	rt := runtimeForTest(t)
	ctx := context.Background()
	cfg := capturedConfig
	cfg.SenderKeyUpdateDelayMs = 150
	c := newCall(t, cfg)
	c.quiet = true
	alice := newPartyOpts(t, rt, "alice", "100000000000001", "aliceCnameAAAAAA", 3, true)
	bob := newPartyOpts(t, rt, "bob", "100000000000002", "bobCnameBBBBBBBB", 7, true)
	c.join(alice, bob)
	c.settle(200*time.Millisecond, 5*time.Second)

	const tracks, frames = 6, 1500
	var wg sync.WaitGroup
	errs := make(chan error, tracks+1)
	for tr := 0; tr < tracks; tr++ {
		audio := tr%2 == 0
		enc, err := alice.km.NewFrameEncryptor(ctx)
		if err != nil {
			t.Fatal(err)
		}
		dec, err := bob.km.NewFrameDecryptor(ctx, alice.e2eeID(), DecryptorHandlers(audio))
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			size := 160
			if !audio {
				size = 8 << 10
			}
			for i := 0; i < frames; i++ {
				f := bytes.Repeat([]byte{byte(tr), byte(i), byte(i >> 8)}, size/3)
				ct, err := enc.Encrypt(ctx, HandlerGeneric, f)
				if err != nil {
					errs <- fmt.Errorf("track %d frame %d: encrypt: %w", tr, i, err)
					return
				}
				pt, err := dec.Decrypt(ctx, ct)
				if err != nil {
					errs <- fmt.Errorf("track %d frame %d: decrypt: %w", tr, i, err)
					return
				}
				if !bytes.Equal(pt, f) {
					errs <- fmt.Errorf("track %d frame %d: mismatch", tr, i)
					return
				}
				if i%100 == 0 {
					_ = alice.km.E2EEEnabled()
				}
			}
		}()
	}
	// Membership churn on the test goroutine (the call simulator is not
	// itself concurrent), while the tracks run.
	cycles := 0
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
loop:
	for i := 0; ; i++ {
		select {
		case <-done:
			break loop
		case err := <-errs:
			t.Fatal(err)
		default:
		}
		carol := newPartyOpts(t, rt, "carol", fmt.Sprintf("1000000000001%02d", i), "carolCnameCCCCCC", 5, true)
		c.join(carol)
		c.settle(50*time.Millisecond, 5*time.Second)
		c.leave(carol)
		c.settle(250*time.Millisecond, 5*time.Second)
		carol.in.Close(ctx)
		cycles++
	}
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	t.Logf("%d tracks × %d frames, %d join/leave cycles, %d key messages relayed meanwhile", tracks, frames, cycles, c.relayed)
	if cycles == 0 {
		t.Error("no membership change happened during the frame traffic")
	}
}
