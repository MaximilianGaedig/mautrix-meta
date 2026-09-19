package framecrypt

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
)

// benchPair sets up alice → bob with one encryptor and an audio and a
// video decryptor.
func benchPair(b *testing.B) (*FrameEncryptor, *FrameDecryptor, *FrameDecryptor) {
	rt := runtimeForBench(b)
	ctx := context.Background()
	alice, bob := benchParties(b, rt)
	enc, err := alice.km.NewFrameEncryptor(ctx)
	if err != nil {
		b.Fatal(err)
	}
	da, err := bob.km.NewFrameDecryptor(ctx, alice.e2eeID(), DecryptorHandlers(true))
	if err != nil {
		b.Fatal(err)
	}
	dv, err := bob.km.NewFrameDecryptor(ctx, alice.e2eeID(), DecryptorHandlers(false))
	if err != nil {
		b.Fatal(err)
	}
	return enc, da, dv
}

func BenchmarkFrames(b *testing.B) {
	enc, da, dv := benchPair(b)
	ctx := context.Background()
	for _, c := range []struct {
		name string
		size int
		h    FrameDataHandlerType
		dec  *FrameDecryptor
	}{
		{"audio-160B", 160, HandlerGeneric, da},
		{"video-15KB", 15 << 10, HandlerGeneric, dv},
		{"video-keyframe-100KB", 100 << 10, HandlerGeneric, dv},
		{"h264-15KB", 15 << 10, HandlerH264, dv},
	} {
		frame := make([]byte, c.size)
		rand.Read(frame)
		if c.h == HandlerH264 {
			frame = h264AccessUnit(c.size)
		}
		b.Run("encrypt-"+c.name, func(b *testing.B) {
			b.SetBytes(int64(len(frame)))
			for i := 0; i < b.N; i++ {
				if _, err := enc.Encrypt(ctx, c.h, frame); err != nil {
					b.Fatal(err)
				}
			}
		})
		// Decrypt needs fresh ciphertexts (replays are rejected).
		b.Run("decrypt-"+c.name, func(b *testing.B) {
			cts := make([][]byte, b.N)
			for i := range cts {
				ct, err := enc.Encrypt(ctx, c.h, frame)
				if err != nil {
					b.Fatal(err)
				}
				cts[i] = ct
			}
			b.SetBytes(int64(len(frame)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := c.dec.Decrypt(ctx, cts[i]); err != nil {
					b.Fatal(fmt.Errorf("frame %d: %w", i, err))
				}
			}
		})
	}
}

func benchParties(b *testing.B, rt *Runtime) (*party, *party) {
	c := newCall(b, capturedConfig)
	c.quiet = true
	alice := newParty(b, rt, "alice", "100000000000001", "aliceCnameAAAAAA", 3)
	bob := newParty(b, rt, "bob", "100000000000002", "bobCnameBBBBBBBB", 7)
	c.join(alice, bob)
	c.relay()
	return alice, bob
}
