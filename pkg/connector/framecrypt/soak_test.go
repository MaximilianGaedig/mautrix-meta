package framecrypt

import (
	"bytes"
	"context"
	"crypto/rand"
	"runtime"
	"testing"
	"time"
)

type memStats struct {
	wasmBytes  uint32 // linear memory size
	objects    uint32 // debug_getAllocatedObjectCount
	goHeap     uint64 // HeapAlloc after GC
	goRoutines int
}

func (in *Instance) objectCount(t testing.TB) uint32 {
	var n uint32
	err := in.do(context.Background(), func(ctx context.Context) (err error) {
		n, err = in.call32(ctx, "debug_getAllocatedObjectCount")
		return
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func sample(t testing.TB, ins ...*Instance) []memStats {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	out := make([]memStats, len(ins))
	for i, in := range ins {
		out[i] = memStats{in.mod.Memory().Size(), in.objectCount(t), ms.HeapAlloc, runtime.NumGoroutine()}
	}
	return out
}

// TestSoakMemory encrypts and decrypts 100k audio and 10k video frames
// (15-25 KB delta frames, a 100 KB keyframe every 100th) through one pair
// of instances and checks that neither the modules' memory nor their
// object counts nor the Go heap grow after warm-up.
func TestSoakMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test skipped in -short mode")
	}
	rt := runtimeForTest(t)
	ctx := context.Background()
	alice, bob, _, _, _ := setupPair(t, rt)
	encA, _ := alice.km.NewFrameEncryptor(ctx)
	encV, _ := alice.km.NewFrameEncryptor(ctx)
	decA, _ := bob.km.NewFrameDecryptor(ctx, alice.e2eeID(), DecryptorHandlers(true))
	decV, _ := bob.km.NewFrameDecryptor(ctx, alice.e2eeID(), DecryptorHandlers(false))
	audio := make([]byte, 200)
	video := make([]byte, 100<<10)
	rand.Read(audio)
	rand.Read(video)

	const audioFrames, videoFrames = 100000, 10000
	var bytesDone int64
	frameErr := func(kind string, i int, err error) {
		t.Helper()
		t.Fatalf("%s frame %d: %v", kind, i, err)
	}
	run := func(from, to int) {
		for i := from; i < to; i++ {
			// ~10 audio frames per video frame, as at 50/s vs 30/s... close enough.
			a := audio[:120+i%80]
			ct, err := encA.Encrypt(ctx, HandlerGeneric, a)
			if err != nil {
				frameErr("audio", i, err)
			}
			pt, err := decA.Decrypt(ctx, ct)
			if err != nil || !bytes.Equal(pt, a) {
				frameErr("audio", i, err)
			}
			bytesDone += int64(len(a))
			if i%10 == 0 {
				j := i / 10
				v := video[:15<<10+(j*977)%(10<<10)]
				if j%100 == 0 {
					v = video // keyframe
				}
				ct, err := encV.Encrypt(ctx, HandlerGeneric, v)
				if err != nil {
					frameErr("video", j, err)
				}
				pt, err := decV.Decrypt(ctx, ct)
				if err != nil || !bytes.Equal(pt, v) {
					frameErr("video", j, err)
				}
				bytesDone += int64(len(v))
			}
		}
	}
	start := time.Now()
	run(0, 2000) // warm-up: includes keyframes, scratch buffers grow
	warm := sample(t, alice.in, bob.in)
	run(2000, audioFrames)
	end := sample(t, alice.in, bob.in)
	el := time.Since(start)
	t.Logf("%d audio + %d video frames encrypted and decrypted in %s (%.1f MB)", audioFrames, videoFrames, el.Round(time.Millisecond), float64(bytesDone)/1e6)
	for i, name := range []string{"alice (encrypting)", "bob (decrypting)"} {
		w, e := warm[i], end[i]
		t.Logf("%s: wasm memory %d → %d bytes, module objects %d → %d", name, w.wasmBytes, e.wasmBytes, w.objects, e.objects)
		if e.wasmBytes != w.wasmBytes {
			t.Errorf("%s: wasm memory grew %d → %d after warm-up", name, w.wasmBytes, e.wasmBytes)
		}
		if e.objects != w.objects {
			t.Errorf("%s: module object count %d → %d", name, w.objects, e.objects)
		}
	}
	t.Logf("Go heap after GC %d → %d bytes, goroutines %d → %d", warm[0].goHeap, end[0].goHeap, warm[0].goRoutines, end[0].goRoutines)
	if end[0].goHeap > warm[0].goHeap+4<<20 {
		t.Errorf("Go heap grew by more than 4 MiB: %d → %d", warm[0].goHeap, end[0].goHeap)
	}

	// Control: the measurement does catch a leak of this order. Leaking
	// 200 bytes per audio frame (what one missed free of a frame-sized
	// buffer would do) must grow the module's memory.
	err := alice.in.do(ctx, func(ctx context.Context) error {
		for i := 0; i < audioFrames; i++ {
			if _, err := alice.in.malloc(ctx, audio); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	leaked := sample(t, alice.in)[0]
	t.Logf("control: after leaking %d × %d bytes, wasm memory %d → %d bytes", audioFrames, len(audio), end[0].wasmBytes, leaked.wasmBytes)
	if leaked.wasmBytes <= end[0].wasmBytes {
		t.Error("control leak not visible in the wasm memory size; the check above proves nothing")
	}
}
