package framecrypt

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"go.mau.fi/libsignal/ecc"

	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
)

func loadWasm(t testing.TB) []byte {
	t.Helper()
	path := os.Getenv("FRAMECRYPT_WASM")
	if path == "" {
		t.Skip("FRAMECRYPT_WASM not set (path to Messenger's frame_encryption .wasm)")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

var (
	sharedRT     *Runtime
	sharedRTErr  error
	sharedRTOnce sync.Once
	testLog      struct {
		sync.Mutex
		t *testing.T
	}
)

// runtimeForTest compiles the module once per test binary; compiling it
// takes most of a test's time.
func runtimeForTest(t *testing.T) *Runtime {
	return runtimeForTB(t, t)
}

func runtimeForBench(b *testing.B) *Runtime { return runtimeForTB(b, nil) }

func runtimeForTB(t testing.TB, logT *testing.T) *Runtime {
	t.Helper()
	wasm := loadWasm(t)
	sharedRTOnce.Do(func() {
		sharedRT, sharedRTErr = NewRuntime(context.Background(), wasm, &Options{Log: func(level, msg string) {
			testLog.Lock()
			defer testLog.Unlock()
			if testLog.t != nil && (level != "INFO" || os.Getenv("FRAMECRYPT_VERBOSE") != "") {
				testLog.t.Logf("wasm [%s] %s", level, msg)
			}
		}})
	})
	if sharedRTErr != nil {
		t.Fatal(sharedRTErr)
	}
	testLog.Lock()
	testLog.t = logT
	testLog.Unlock()
	t.Cleanup(func() {
		testLog.Lock()
		testLog.t = nil
		testLog.Unlock()
	})
	return sharedRT
}

func TestRewriteInstantiates(t *testing.T) {
	wasm := loadWasm(t)
	out, info, err := rewriteModule(wasm, 7)
	if err != nil {
		t.Fatal(err)
	}
	if info.MemoryMin != 87 || info.MemoryMax != 32768 {
		t.Errorf("memory limits %d..%d, want 87..32768 (env.memory import)", info.MemoryMin, info.MemoryMax)
	}
	if info.TableSize != info.TableBase+7 {
		t.Errorf("table %d → %d, want +7", info.TableBase, info.TableSize)
	}
	// Every section other than import/table/memory must be untouched.
	orig, _ := splitSections(wasm)
	rew, err := splitSections(out)
	if err != nil {
		t.Fatal(err)
	}
	var o, r []wasmSection
	for _, s := range orig {
		if s.id != secImport && s.id != secTable {
			o = append(o, s)
		}
	}
	for _, s := range rew {
		if s.id != secImport && s.id != secTable && s.id != secMemory {
			r = append(r, s)
		}
	}
	if len(o) != len(r) {
		t.Fatalf("section count %d → %d", len(o), len(r))
	}
	for i := range o {
		if o[i].id != r[i].id || !bytes.Equal(o[i].body, r[i].body) {
			t.Fatalf("section %d (id %d) changed", i, o[i].id)
		}
	}
	rt := runtimeForTest(t)
	ctx := context.Background()
	in, err := rt.NewInstance(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close(ctx)
	if mem := in.mod.Memory(); mem == nil || mem.Size() < 87*65536 {
		t.Fatal("rewritten module has no defined memory")
	}
	// The table slots the shim filled are reachable from the module: a
	// log callback slot exists and resolves.
	if in.logSlot < rt.info.TableBase || in.logSlot >= rt.info.TableSize {
		t.Fatalf("log slot %d outside the added range", in.logSlot)
	}
	if len(in.logLevels) != 3 {
		t.Fatalf("log levels %v", in.logLevels)
	}
}

// party is one call participant with its own module instance.
type party struct {
	name    string
	userID  string
	cname   string
	devID   int32
	keyPair *ecc.ECKeyPair
	in      *Instance
	ctx     *E2eeContext
	km      *KeysManager

	mu      sync.Mutex
	out     []sent
	sctpOut []sent
	peers   map[string][]byte // userID → serialized identity key
}

type sent struct {
	to   string
	data []byte
}

func (p *party) e2eeID() string { return p.userID + ":" + p.cname }

func newParty(t testing.TB, rt *Runtime, name, userID, cname string, devID int32) *party {
	t.Helper()
	return newPartyOpts(t, rt, name, userID, cname, devID, false)
}

func newPartyOpts(t testing.TB, rt *Runtime, name, userID, cname string, devID int32, follow bool) *party {
	t.Helper()
	ctx := context.Background()
	kp, err := ecc.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	p := &party{name: name, userID: userID, cname: cname, devID: devID, keyPair: kp, peers: map[string][]byte{}}
	if p.in, err = rt.NewInstance(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.in.Close(context.Background()) })
	p.ctx, err = p.in.NewContext(ctx, ContextConfig{
		E2eeMandated: true,
		Identity: IdentityStore{
			LocalUserID:   userID,
			LocalDeviceID: devID,
			KeyMode:       IdentityKeyPersistentValidate,
			LocalIdentityKey: func() ([]byte, []byte) {
				priv := kp.PrivateKey().Serialize()
				return kp.PublicKey().Serialize(), priv[:]
			},
			RemoteIdentityKey: func(user string, dev int32) []byte {
				p.mu.Lock()
				defer p.mu.Unlock()
				return p.peers[user]
			},
			SaveRemoteIdentityKey: func(user string, dev int32, key []byte) {
				t.Logf("%s: module saved identity key for %s/%d (%d bytes)", name, user, dev, len(key))
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.km, err = p.ctx.NewKeysManager(ctx, KeysManagerConfig{
		UserID:         userID,
		E2eeMandated:   true,
		ManualKeyIndex: !follow,
		SendE2eeMessage: func(to string, data []byte) {
			p.mu.Lock()
			p.out = append(p.out, sent{to, data})
			p.mu.Unlock()
		},
		SctpSendE2eeMessage: func(to string, data []byte) {
			p.mu.Lock()
			p.sctpOut = append(p.sctpOut, sent{to, data})
			p.mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.km.SetLocalE2eeID(ctx, p.e2eeID()); err != nil {
		t.Fatal(err)
	}
	return p
}

func (p *party) takeOut() []sent {
	p.mu.Lock()
	defer p.mu.Unlock()
	o := p.out
	p.out = nil
	return o
}

func (p *party) clientState(t testing.TB) *rtcsignal.E2eeClientState {
	t.Helper()
	raw, err := p.km.SerializedE2eeClientState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cs, err := rtcsignal.ParseE2eeClientState(raw)
	if err != nil {
		t.Fatal(err)
	}
	return cs
}

// setupPair creates alice and bob, feeds each a server state listing the
// other, and relays their E2eeKey messages until both are quiet. It returns
// alice's client state, the last key index each side reported, and every
// relayed message.
func setupPair(t *testing.T, rt *Runtime) (alice, bob *party, acs *rtcsignal.E2eeClientState, keyIndex map[*party]string, relayed []sent) {
	ctx := context.Background()
	alice = newParty(t, rt, "alice", "100000000000001", "aliceCnameAAAAAA", 3)
	bob = newParty(t, rt, "bob", "100000000000002", "bobCnameBBBBBBBB", 7)
	// Each side's identity store knows the other's identity key, as
	// ZenonIdentityStoreInMemoryCache.cacheIdentityKeys does from the
	// endpointInfos before the server state is processed.
	alice.peers[bob.userID] = bob.keyPair.PublicKey().Serialize()
	bob.peers[alice.userID] = alice.keyPair.PublicKey().Serialize()

	acs = alice.clientState(t)
	bcs := bob.clientState(t)
	for _, c := range []struct {
		p  *party
		cs *rtcsignal.E2eeClientState
	}{{alice, acs}, {bob, bcs}} {
		b, err := rtcsignal.ParsePreKeyBundle(c.cs.PreKeyBundle)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b.IdentityKey, c.p.keyPair.PublicKey().Serialize()) {
			t.Fatalf("%s: client state bundle does not carry the identity key from the store", c.p.name)
		}
		if ok, err := b.VerifySignedPreKey(); !ok || err != nil {
			t.Fatalf("%s: signed prekey does not verify: %v %v", c.p.name, ok, err)
		}
		t.Logf("%s client state: suites=%v versions=%v mode=%d idmode=%d dev=%d prot=%v bundle=%dB",
			c.p.name, c.cs.CipherSuites, c.cs.SupportedVersions, c.cs.KeyNegotiationMode,
			c.cs.IdentityKeyMode, c.cs.DeviceID, c.cs.KeyNegotiationProt, len(c.cs.PreKeyBundle))
	}

	toAlice := buildServerState(map[string]testEndpoint{bob.e2eeID(): {PreKeyBundle: bcs.PreKeyBundle, IdentityKeyMode: bcs.IdentityKeyMode, DeviceID: bcs.DeviceID}})
	toBob := buildServerState(map[string]testEndpoint{alice.e2eeID(): {PreKeyBundle: acs.PreKeyBundle, IdentityKeyMode: acs.IdentityKeyMode, DeviceID: acs.DeviceID}})
	keyIndex = map[*party]string{}
	for _, c := range []struct {
		p     *party
		state []byte
	}{{alice, toAlice}, {bob, toBob}} {
		res, err := c.p.km.ProcessE2eeServerUpdate(ctx, c.state)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s server update: err=%d keyIndex=%q delay=%s removeDelay=%d", c.p.name, res.ErrorCode, res.KeyIndex, res.KeyUpdateDelay, res.RemoveFrameDecryptorDelayMs)
		if res.ErrorCode != 0 {
			t.Fatalf("%s: E2EE not negotiated (error code %d)", c.p.name, res.ErrorCode)
		}
		keyIndex[c.p] = res.KeyIndex
	}

	// Relay E2eeKey messages until both sides are quiet, as the SFU does
	// with DATA_MESSAGE topic E2eeKey.
	byUser := map[string]*party{alice.userID: alice, bob.userID: bob}
	for round := 0; round < 10; round++ {
		moved := 0
		for _, from := range []*party{alice, bob} {
			for _, m := range from.takeOut() {
				uid, _, _ := strings.Cut(m.to, ":")
				to := byUser[uid]
				if to == nil || to == from {
					t.Fatalf("%s sent an E2eeKey message to unknown recipient %q", from.name, m.to)
				}
				relayed = append(relayed, m)
				res, err := to.km.ProcessE2eeMessage(ctx, m.data)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("round %d: %s → %s (recipient %q, %d bytes): status=%d keyIndex=%q delay=%s",
					round, from.name, to.name, redactID(m.to), len(m.data), res.StatusCode, res.KeyIndex, res.KeyUpdateDelay)
				if res.KeyIndex != "" {
					keyIndex[to] = res.KeyIndex
				}
				moved++
			}
		}
		if moved == 0 {
			break
		}
	}

	return
}

func TestTwoPartyFrameExchange(t *testing.T) {
	rt := runtimeForTest(t)
	ctx := context.Background()
	alice, bob, acs, keyIndex, _ := setupPair(t, rt)

	// Activate the sender key now instead of after keyUpdateDelay
	// (ZenonEncryptionKeysManager.$11 calls updateSenderKeyIndex on a timer).
	for _, p := range []*party{alice, bob} {
		if ki := keyIndex[p]; ki != "" && ki != "0" {
			next, err := p.km.UpdateSenderKeyIndex(ctx, ki)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%s: activated key index %q, next %q", p.name, ki, next)
		}
	}

	enc, err := alice.km.NewFrameEncryptor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	decAudio, err := bob.km.NewFrameDecryptor(ctx, alice.e2eeID(), DecryptorHandlers(true))
	if err != nil {
		t.Fatal(err)
	}
	decVideo, err := bob.km.NewFrameDecryptor(ctx, alice.e2eeID(), DecryptorHandlers(false))
	if err != nil {
		t.Fatal(err)
	}

	// An Opus-like packet: TOC byte (SILK WB 20 ms, mono) plus payload.
	opus := append([]byte{0x48}, bytes.Repeat([]byte{0x5a, 0xc3, 0x11, 0x07}, 20)...)
	// A VP8-like key frame: 3-byte frame tag (key frame, show_frame, first
	// partition size), start code 9d 01 2a, 640x480, then payload.
	vp8 := append([]byte{0x50, 0x42, 0x00, 0x9d, 0x01, 0x2a, 0x80, 0x02, 0xe0, 0x01},
		bytes.Repeat([]byte{0x01, 0x80, 0x33, 0xfe, 0x00}, 60)...)

	for _, c := range []struct {
		name  string
		frame []byte
		h     FrameDataHandlerType
		dec   *FrameDecryptor
	}{
		{"opus", opus, HandlerForCodec(true, "opus"), decAudio},
		{"vp8", vp8, HandlerForCodec(false, "VP8"), decVideo},
	} {
		ct, err := enc.Encrypt(ctx, c.h, c.frame)
		if err != nil {
			t.Fatalf("%s: encrypt: %v", c.name, err)
		}
		if bytes.Contains(ct, c.frame[len(c.frame)-16:]) {
			t.Fatalf("%s: ciphertext contains plaintext", c.name)
		}
		t.Logf("%s: %d → %d bytes, plain head % x, cipher head % x, tail % x", c.name, len(c.frame), len(ct), c.frame[:6], ct[:6], ct[len(ct)-12:])
		pt, err := c.dec.Decrypt(ctx, ct)
		if err != nil {
			t.Fatalf("%s: decrypt: %v", c.name, err)
		}
		if !bytes.Equal(pt, c.frame) {
			t.Fatalf("%s: decrypted frame differs", c.name)
		}
		// Tampering with the ciphertext must fail authentication.
		ct2, err := enc.Encrypt(ctx, c.h, c.frame)
		if err != nil {
			t.Fatal(err)
		}
		// Frame layout seen here: ciphertext, 8-byte tag, then a 3-byte
		// trailer (key index, counter, trailer length 3). Flipping a bit
		// in the ciphertext or tag must fail authentication; corrupting
		// the trailer length makes the frame unparsable.
		for _, tc := range []struct {
			pos  int
			want FrameError
		}{
			{12, ErrCipherAuth},
			{len(ct2) / 2, ErrCipherAuth},
			{len(ct2) - 5, ErrCipherAuth}, // inside the tag
			{len(ct2) - 1, ErrParse},      // trailer length byte
		} {
			bad := bytes.Clone(ct2)
			bad[tc.pos] ^= 0x01
			pt, err := c.dec.Decrypt(ctx, bad)
			if err == nil {
				t.Fatalf("%s: frame tampered at byte %d decrypted (%d bytes)", c.name, tc.pos, len(pt))
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("%s: frame tampered at byte %d: %v, want %v", c.name, tc.pos, err, tc.want)
			}
			t.Logf("%s: frame tampered at byte %d/%d rejected: %v", c.name, tc.pos, len(bad), err)
		}
		// The untouched copy still decrypts (the failure did not poison
		// the decryptor or the instance).
		if pt, err := c.dec.Decrypt(ctx, ct2); err != nil || !bytes.Equal(pt, c.frame) {
			t.Fatalf("%s: second frame: %v", c.name, err)
		}
	}
	// A different participant's decryptor must not decrypt alice's frames.
	ct, err := enc.Encrypt(ctx, HandlerGeneric, opus)
	if err != nil {
		t.Fatal(err)
	}
	wrong, err := bob.km.NewFrameDecryptor(ctx, bob.e2eeID(), DecryptorHandlers(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = wrong.Decrypt(ctx, ct); !errors.Is(err, ErrMissingKey) {
		t.Fatalf("frame with another sender's decryptor: %v, want ERROR_MISSING_KEY", err)
	} else {
		t.Logf("another sender's decryptor: %v", err)
	}
	t.Logf("timers started: alice %d, bob %d", alice.in.timerSeq, bob.in.timerSeq)

	// Negative control: a participant who got the server state but none of
	// alice's E2eeKey messages cannot decrypt her frames, so the exchange
	// above is what made decryption work.
	carol := newParty(t, rt, "carol", "100000000000003", "carolCnameCCCCCC", 5)
	carol.peers[alice.userID] = alice.keyPair.PublicKey().Serialize()
	if res, err := carol.km.ProcessE2eeServerUpdate(ctx, buildServerState(map[string]testEndpoint{
		alice.e2eeID(): {PreKeyBundle: acs.PreKeyBundle, IdentityKeyMode: acs.IdentityKeyMode, DeviceID: acs.DeviceID},
	})); err != nil || res.ErrorCode != 0 {
		t.Fatalf("carol server update: %v %+v", err, res)
	}
	carolDec, err := carol.km.NewFrameDecryptor(ctx, alice.e2eeID(), DecryptorHandlers(true))
	if err != nil {
		t.Fatal(err)
	}
	if pt, err := carolDec.Decrypt(ctx, ct); err == nil {
		t.Fatalf("carol decrypted alice's frame without her key (%d bytes)", len(pt))
	} else if !errors.Is(err, ErrMissingKey) {
		t.Fatalf("carol without alice's key: %v, want ERROR_MISSING_KEY", err)
	} else {
		t.Logf("carol without alice's key: %v", err)
	}

	for _, x := range []interface{ Close(context.Context) error }{carolDec, carol.km, carol.ctx, enc, decAudio, decVideo, wrong, alice.km, bob.km, alice.ctx, bob.ctx} {
		if err := x.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func redactID(s string) string {
	uid, cname, ok := strings.Cut(s, ":")
	if !ok {
		return s
	}
	return uid + ":<" + strings.Repeat("x", len(cname)) + ">"
}

// TestServerStateMatchesCapture checks that buildServerState reproduces
// the captured E2eeServerState blobs byte for byte, so the acceptance test
// feeds the module what the server really sends.
func TestServerStateMatchesCapture(t *testing.T) {
	h := loadHARE2ee(t)
	n := 0
	for _, s := range h.States {
		if s.Dir != "receive" {
			continue
		}
		st, err := rtcsignal.ParseE2eeServerState(s.Data)
		if err != nil {
			t.Fatal(err)
		}
		eps := map[string]testEndpoint{}
		for k, e := range st.EndpointInfos {
			eps[k] = testEndpoint{PreKeyBundle: e.PreKeyBundle, IdentityKeyMode: e.IdentityKeyMode, DeviceID: e.DeviceID}
		}
		if got := buildServerState(eps); !bytes.Equal(got, s.Data) {
			t.Fatalf("server state %d (%d bytes) differs from the rebuilt one (%d bytes):\n%s\nvs\n%s",
				n, len(s.Data), len(got), dumpCompact(s.Data), dumpCompact(got))
		}
		n++
	}
	if n == 0 {
		t.Fatal("no server E2eeState in the capture")
	}
	t.Logf("%d captured server states reproduced", n)
}

// TestClientStateMatchesCapture checks that the E2eeClientState the module
// produces here has the same structure (field ids, types, lengths and
// scalar values) as the one the web client sent in the captured call, and
// so does the preKeyBundle inside it. Binary values are compared by length
// only.
func TestClientStateMatchesCapture(t *testing.T) {
	h := loadHARE2ee(t)
	rt := runtimeForTest(t)
	var captured []byte
	for _, s := range h.States {
		if s.Dir == "send" {
			captured = s.Data
			break
		}
	}
	if captured == nil {
		t.Fatal("no client E2eeState in the capture")
	}
	cs, err := rtcsignal.ParseE2eeClientState(captured)
	if err != nil {
		t.Fatal(err)
	}
	p := newParty(t, rt, "local", "100000000000001", "localCnameLLLLLL", cs.DeviceID)
	ours, err := p.km.SerializedE2eeClientState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if a, b := dumpCompact(ours), dumpCompact(captured); a != b {
		t.Fatalf("client state structure differs:\nours %s\ncaptured %s", a, b)
	}
	ourCS, _ := rtcsignal.ParseE2eeClientState(ours)
	if a, b := dumpCompact(ourCS.PreKeyBundle), dumpCompact(cs.PreKeyBundle); a != b {
		t.Fatalf("preKeyBundle structure differs:\nours %s\ncaptured %s", a, b)
	}
	t.Logf("client state: %d bytes, same structure as the captured %d bytes", len(ours), len(captured))
}

// TestE2eeKeyMessagesMatchCapture checks that the E2eeKey messages two
// instances exchange have the structure of the ones in the captured call
// (one key message and one ack per direction).
func TestE2eeKeyMessagesMatchCapture(t *testing.T) {
	h := loadHARE2ee(t)
	rt := runtimeForTest(t)
	_, _, _, _, relayed := setupPair(t, rt)
	shapes := map[string]bool{}
	for _, k := range h.Keys {
		shapes[dumpCompact(k.Data)] = true
	}
	for _, m := range relayed {
		d := dumpCompact(m.data)
		if !shapes[d] {
			t.Errorf("relayed message (%d bytes) has no structural match in the capture:\n%s", len(m.data), d)
		}
	}
	if len(relayed) != len(h.Keys) {
		t.Errorf("relayed %d messages, capture has %d", len(relayed), len(h.Keys))
	}
}
