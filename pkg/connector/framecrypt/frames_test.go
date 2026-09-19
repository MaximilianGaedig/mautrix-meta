package framecrypt

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

// h264AccessUnit is an Annex B IDR access unit: SPS, PPS, IDR slice.
func h264AccessUnit(sliceLen int) []byte {
	sc := []byte{0, 0, 0, 1}
	sps := []byte{0x67, 0x42, 0xc0, 0x1f, 0xda, 0x01, 0x40, 0x16, 0xec, 0x04, 0x40, 0x00, 0x00, 0x03, 0x00, 0x40, 0x00, 0x00, 0x0f, 0x03, 0xc6, 0x0c, 0xa8}
	pps := []byte{0x68, 0xce, 0x3c, 0x80}
	idr := []byte{0x65, 0x88, 0x84, 0x00}
	for i := 0; len(idr) < sliceLen; i++ {
		idr = append(idr, byte(0x11+i*7)|0x01) // no 00 00 0x sequences
	}
	var out []byte
	for _, n := range [][]byte{sps, pps, idr} {
		out = append(append(out, sc...), n...)
	}
	return out
}

// nalHeaders returns the NAL header bytes following each 4-byte start
// code.
func nalHeaders(b []byte) []byte {
	var out []byte
	for i := 0; i+4 < len(b); i++ {
		if b[i] == 0 && b[i+1] == 0 && b[i+2] == 0 && b[i+3] == 1 {
			out = append(out, b[i+4])
		}
	}
	return out
}

func TestH264Handler(t *testing.T) {
	rt := runtimeForTest(t)
	ctx := context.Background()
	alice, bob, _, _, _ := setupPair(t, rt)
	enc, err := alice.km.NewFrameEncryptor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := bob.km.NewFrameDecryptor(ctx, alice.e2eeID(), DecryptorHandlers(false))
	if err != nil {
		t.Fatal(err)
	}
	au := h264AccessUnit(2000)
	h := HandlerForCodec(false, "H264")
	if h != HandlerH264 {
		t.Fatalf("HandlerForCodec(video, H264) = %d", h)
	}
	ct, err := enc.Encrypt(ctx, h, au)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("H264 AU %d → %d bytes; NAL headers plain % x, encrypted % x", len(au), len(ct), nalHeaders(au), nalHeaders(ct))
	t.Logf("encrypted head % x", ct[:40])
	// The H264 handler keeps start codes and NAL headers in the clear
	// (an SFU can still see NAL types and keyframes).
	if !bytes.Equal(nalHeaders(ct), nalHeaders(au)) {
		t.Fatalf("NAL headers not preserved: % x vs % x", nalHeaders(ct), nalHeaders(au))
	}
	if bytes.Contains(ct, au[len(au)-64:]) {
		t.Fatal("IDR slice data left in the clear")
	}
	pt, err := dec.Decrypt(ctx, ct)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pt, au) {
		t.Fatal("H264 AU did not round-trip")
	}
	// The same AU with the Generic handler: nothing stays in the clear,
	// and the video decryptor (Generic+H264) still takes it.
	ctg, err := enc.Encrypt(ctx, HandlerGeneric, au)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(nalHeaders(ctg), nalHeaders(au)) {
		t.Error("Generic handler left the NAL structure in the clear")
	}
	if pt, err = dec.Decrypt(ctx, ctg); err != nil || !bytes.Equal(pt, au) {
		t.Fatalf("Generic-encrypted AU at the video decryptor: %v", err)
	}
	// An audio decryptor (Generic only) does not accept H264 frames.
	adec, err := bob.km.NewFrameDecryptor(ctx, alice.e2eeID(), DecryptorHandlers(true))
	if err != nil {
		t.Fatal(err)
	}
	ct2, _ := enc.Encrypt(ctx, HandlerH264, au)
	if _, err = adec.Decrypt(ctx, ct2); err == nil {
		t.Error("audio (Generic-only) decryptor decrypted an H264-handler frame")
	} else {
		t.Logf("H264 frame at a Generic-only decryptor: %v", err)
	}
	// Non-H264 data with the H264 handler: the module falls back to
	// Generic (frameEncryptor_encrypt retries with handler 0 on status 26).
	vp8 := append([]byte{0x50, 0x42, 0x00, 0x9d, 0x01, 0x2a}, bytes.Repeat([]byte{0x33}, 500)...)
	ct3, err := enc.Encrypt(ctx, HandlerH264, vp8)
	if err != nil {
		t.Fatalf("non-H264 data with the H264 handler: %v", err)
	}
	if pt, err = dec.Decrypt(ctx, ct3); err != nil || !bytes.Equal(pt, vp8) {
		t.Fatalf("fallback frame: %v", err)
	}
}

// TestE2eeOffPassthrough: when the server update negotiates E2EE off, the
// keys manager does what ZenonEncryptionKeysManager.processE2eeServerState
// and ZenonSecureFrameManager do: frames go out unencrypted (dropped when
// infra-mandated), decryptors accept unencrypted frames, and after
// removeFrameDecryptorDelayMs frames are no longer decrypted at all. A
// later successful update turns everything back on.
func TestE2eeOffPassthrough(t *testing.T) {
	rt := runtimeForTest(t)
	ctx := context.Background()
	cfg := capturedConfig
	cfg.RemoveFrameDecryptorDelayMs = 150 // captured: 15000
	c := newCall(t, cfg)
	alice := newParty(t, rt, "alice", "100000000000001", "aliceCnameAAAAAA", 3)
	bob := newParty(t, rt, "bob", "100000000000002", "bobCnameBBBBBBBB", 7)
	c.join(alice, bob)
	c.settle(300*time.Millisecond, 5*time.Second)
	mandated, err := alice.ctx.NewKeysManager(ctx, KeysManagerConfig{UserID: alice.userID, E2eeMandated: true, InfraMandated: true})
	if err != nil {
		t.Fatal(err)
	}
	enc, _ := alice.km.NewFrameEncryptor(ctx)
	menc, _ := mandated.NewFrameEncryptor(ctx)
	dec, _ := bob.km.NewFrameDecryptor(ctx, alice.e2eeID(), DecryptorHandlers(true))
	frame := []byte("plain opus frame, e2ee off")

	// Server turns E2EE off: an unsupported cipher suite.
	off := cfg
	off.CipherSuite = 99
	for _, km := range []*KeysManager{alice.km, bob.km, mandated} {
		res, err := km.ProcessE2eeServerUpdate(ctx, buildServerStateWith(off, nil))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("server update with cipher suite 99: errorCode=%d removeDelay=%dms", res.ErrorCode, res.RemoveFrameDecryptorDelayMs)
		if res.ErrorCode == 0 || km.E2EEEnabled() {
			t.Fatalf("E2EE still negotiated on (errorCode %d)", res.ErrorCode)
		}
	}
	out, err := enc.Encrypt(ctx, HandlerGeneric, frame)
	if err != nil || !bytes.Equal(out, frame) {
		t.Fatalf("E2EE off, not mandated: Encrypt = %q, %v; want the frame unencrypted", out, err)
	}
	if _, err = menc.Encrypt(ctx, HandlerGeneric, frame); !errors.Is(err, ErrEncryptionOff) {
		t.Fatalf("E2EE off, infra-mandated: Encrypt error %v, want ErrEncryptionOff", err)
	}
	// Within removeFrameDecryptorDelayMs the decryptor runs, with
	// unencrypted data allowed.
	pt, err := dec.Decrypt(ctx, frame)
	if err != nil || !bytes.Equal(pt, frame) {
		t.Fatalf("unencrypted frame at a decryptor allowing unencrypted data: %q, %v", pt, err)
	}
	t.Logf("unencrypted frame passed through the module's decryptor")
	time.Sleep(300 * time.Millisecond)
	// Now decryption is disabled: frames are returned untouched.
	junk := []byte{0xde, 0xad, 0xbe, 0xef, 0x00, 0x01, 0x02}
	if pt, err = dec.Decrypt(ctx, junk); err != nil || !bytes.Equal(pt, junk) {
		t.Fatalf("after removeFrameDecryptorDelayMs: %x, %v; want passthrough", pt, err)
	}

	// Server turns E2EE back on.
	c.pushState()
	c.settle(300*time.Millisecond, 5*time.Second)
	if !alice.km.E2EEEnabled() || !bob.km.E2EEEnabled() {
		t.Fatal("E2EE not re-enabled")
	}
	ct, err := enc.Encrypt(ctx, HandlerGeneric, frame)
	if err != nil || bytes.Equal(ct, frame) {
		t.Fatalf("after re-enable, Encrypt = %v (unencrypted: %v)", err, bytes.Equal(ct, frame))
	}
	if pt, err = dec.Decrypt(ctx, ct); err != nil || !bytes.Equal(pt, frame) {
		t.Fatalf("after re-enable, decrypt: %v", err)
	}
	if err = mandated.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
