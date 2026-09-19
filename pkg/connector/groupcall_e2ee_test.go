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
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"github.com/rs/zerolog"
	"go.mau.fi/libsignal/ecc"
	"maunium.net/go/mautrix/bridgev2"

	"go.mau.fi/mautrix-meta/pkg/connector/callbridge"
	"go.mau.fi/mautrix-meta/pkg/connector/framecrypt"
	"go.mau.fi/mautrix-meta/pkg/connector/framecrypt/fcsim"
	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

var (
	e2eeTestRT     *framecrypt.Runtime
	e2eeTestRTErr  error
	e2eeTestRTOnce sync.Once
)

// e2eeTestRuntime compiles the module FRAMECRYPT_WASM names (Messenger's frame_encryption .wasm).
func e2eeTestRuntime(t *testing.T) *framecrypt.Runtime {
	t.Helper()
	path := os.Getenv("FRAMECRYPT_WASM")
	if path == "" {
		t.Skip("FRAMECRYPT_WASM not set (path to Messenger's frame_encryption .wasm)")
	}
	e2eeTestRTOnce.Do(func() {
		var wasm []byte
		if wasm, e2eeTestRTErr = os.ReadFile(path); e2eeTestRTErr == nil {
			e2eeTestRT, e2eeTestRTErr = framecrypt.NewRuntime(context.Background(), wasm, nil)
		}
	})
	if e2eeTestRTErr != nil {
		t.Fatal(e2eeTestRTErr)
	}
	return e2eeTestRT
}

// e2eeSFU plays Messenger's SFU for groupE2ee members: it hands each an E2eeServerState listing
// the others, and relays E2eeKey messages by user id.
type e2eeSFU struct {
	t       *testing.T
	lock    sync.Mutex
	members map[string]*e2eeMember
	sent    int
}

type e2eeMember struct {
	uid, cname string
	e          *groupE2ee
	state      *rtcsignal.E2eeClientState
	trust      func(userID string, deviceID int32, key []byte) bool
}

func (m *e2eeMember) id() string { return m.uid + ":" + m.cname }

func (s *e2eeSFU) join(uid, cname string, devID int32, trust func(string, int32, []byte) bool) *e2eeMember {
	t := s.t
	t.Helper()
	kp, err := ecc.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	m := &e2eeMember{uid: uid, cname: cname, trust: trust}
	send := func(ctx context.Context, to string, data []byte) error {
		s.lock.Lock()
		dst := s.members[to]
		s.sent++
		s.lock.Unlock()
		if dst == nil {
			return errors.New("recipient not in the call")
		}
		dst.e.keyMessage(&rtcsignal.DataMessage{Sender: uid, Recipients: []string{to}, Topic: rtcsignal.TopicE2eeKey, Data: data})
		return nil
	}
	var trustFn func(context.Context, string, int32, []byte) bool
	if trust != nil {
		trustFn = func(_ context.Context, u string, d int32, k []byte) bool { return trust(u, d, k) }
	}
	e, raw, err := newGroupE2ee(context.Background(), e2eeTestRuntime(t), groupE2eeConfig{
		SelfID:        uid,
		Identity:      &callbridge.Identity{DeviceID: devID, Priv: kp.PrivateKey().Serialize(), Pub: kp.PublicKey().PublicKey()},
		LocalCname:    cname,
		Send:          send,
		TrustIdentity: trustFn,
		Log:           zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.InfoLevel).With().Str("member", uid).Logger(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.close)
	m.e = e
	if m.state, err = rtcsignal.ParseE2eeClientState(raw); err != nil {
		t.Fatal(err)
	}
	s.lock.Lock()
	s.members[uid] = m
	s.lock.Unlock()
	return m
}

// pushState sends every member the server state (in a media update's state store, as the SFU does).
func (s *e2eeSFU) pushState() {
	s.lock.Lock()
	members := make([]*e2eeMember, 0, len(s.members))
	for _, m := range s.members {
		members = append(members, m)
	}
	s.lock.Unlock()
	for _, m := range members {
		eps := map[string]fcsim.Endpoint{}
		for _, o := range members {
			if o != m {
				eps[o.id()] = fcsim.Endpoint{PreKeyBundle: o.state.PreKeyBundle, IdentityKeyMode: o.state.IdentityKeyMode, DeviceID: o.state.DeviceID}
			}
		}
		m.e.serverState(rtcsignal.StateStore{{Topic: rtcsignal.TopicE2eeState, State: rtcsignal.State{Version: 1,
			Data: fcsim.ServerState(fcsim.CapturedConfig, eps)}}}, "test")
	}
}

// rtpPipe connects one relay's output to another's input.
type rtpPipe struct{ ch chan *rtp.Packet }

func newRTPPipe() *rtpPipe { return &rtpPipe{ch: make(chan *rtp.Packet, 4096)} }

func (p *rtpPipe) WriteRTP(pkt *rtp.Packet) error {
	c := *pkt
	c.Payload = append([]byte(nil), pkt.Payload...)
	p.ch <- &c
	return nil
}

func (p *rtpPipe) ReadRTP() (*rtp.Packet, interceptor.Attributes, error) {
	pkt, ok := <-p.ch
	if !ok {
		return nil, nil, io.EOF
	}
	return pkt, nil, nil
}

// waitDecryptable encrypts probe frames at from until to's decryptor for from opens one.
func waitDecryptable(t *testing.T, from, to *e2eeMember, within time.Duration) bool {
	t.Helper()
	enc := from.e.encryptTransform(zerolog.Nop())
	dec, closeFn, err := to.e.decryptor(from.id(), true, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		ct, err := enc([]byte("probe frame"))
		if err == nil {
			if pt, err := dec(ct); err == nil && string(pt) == "probe frame" {
				return true
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func newE2eeSFU(t *testing.T) *e2eeSFU {
	return &e2eeSFU{t: t, members: map[string]*e2eeMember{}}
}

// TestGroupE2eeMedia runs the bridge's side of an encrypted group call against a web client stand-in
// (a second copy of the module): keys are exchanged through the fake SFU, and audio and video
// frames go through the frame relays, encrypted on one side and decrypted on the other, both ways.
func TestGroupE2eeMedia(t *testing.T) {
	sfu := newE2eeSFU(t)
	bridge := sfu.join("100000000000001", "0a6b7c4e-2f1d-4e0a-9b3c-5d6e7f8091a2", 3, nil)
	web := sfu.join("100000000000002", "hZvKyjdfjh6GgP1x", 7, nil)
	sfu.pushState()
	if !waitDecryptable(t, web, bridge, 10*time.Second) || !waitDecryptable(t, bridge, web, 10*time.Second) {
		t.Fatalf("keys weren't exchanged (%d E2eeKey messages sent)", sfu.sent)
	}
	t.Logf("keys exchanged with %d E2eeKey messages", sfu.sent)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	log := zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.InfoLevel)

	// Audio, web → bridge → LiveKit: the web client's encrypted Opus is decrypted by the bridge.
	opus := make([][]byte, 50)
	for i := range opus {
		opus[i] = make([]byte, 40+i)
		_, _ = rand.Read(opus[i])
	}
	src, wire, out := newRTPPipe(), newRTPPipe(), newRTPPipe()
	for i, f := range opus {
		src.ch <- &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SSRC: 1, SequenceNumber: uint16(i), Timestamp: uint32(i * 960)}, Payload: f}
	}
	close(src.ch)
	var encStats, decStats callbridge.FrameRelayStats
	if err := callbridge.RelayAudioTransformed(ctx, src, 111, wire, web.e.encryptTransform(log), &encStats, log); err != nil {
		t.Fatal(err)
	}
	close(wire.ch)
	xf, closeDec, err := bridge.e.decryptor(callbridge.E2eeIDOfStream(web.id()+":stream-uuid"), true, log)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDec()
	if err := callbridge.RelayAudioTransformed(ctx, wire, 111, out, xf, &decStats, log); err != nil {
		t.Fatal(err)
	}
	close(out.ch)
	i := 0
	for p := range out.ch {
		if !bytes.Equal(p.Payload, opus[i]) {
			t.Fatalf("audio frame %d differs after encrypt+decrypt", i)
		}
		i++
	}
	if i != len(opus) || decStats.Failed.Load() != 0 {
		t.Fatalf("audio web→bridge: %d/%d frames, %d failed", i, len(opus), decStats.Failed.Load())
	}

	// Audio, Matrix → bridge → web: the bridge encrypts the user's Opus for the web client.
	src, wire, out = newRTPPipe(), newRTPPipe(), newRTPPipe()
	for i, f := range opus {
		src.ch <- &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SSRC: 2, SequenceNumber: uint16(i), Timestamp: uint32(i * 960)}, Payload: f}
	}
	close(src.ch)
	encStats, decStats = callbridge.FrameRelayStats{}, callbridge.FrameRelayStats{}
	if err := callbridge.RelayAudioTransformed(ctx, src, 111, wire, bridge.e.encryptTransform(log), &encStats, log); err != nil {
		t.Fatal(err)
	}
	close(wire.ch)
	webDec, closeWebDec, err := web.e.decryptor(bridge.id(), true, log)
	if err != nil {
		t.Fatal(err)
	}
	defer closeWebDec()
	ciphertextDiffers := false
	var got [][]byte
	go func() {
		defer close(out.ch)
		_ = callbridge.RelayAudioTransformed(ctx, wire, 111, out, func(ct []byte) ([]byte, error) {
			pt, err := webDec(ct)
			if err == nil && !bytes.Equal(ct, pt) {
				ciphertextDiffers = true
			}
			return pt, err
		}, &decStats, log)
	}()
	for p := range out.ch {
		got = append(got, p.Payload)
	}
	if len(got) != len(opus) || decStats.Failed.Load() != 0 || !ciphertextDiffers {
		t.Fatalf("audio bridge→web: %d/%d frames, %d failed, encrypted=%v", len(got), len(opus), decStats.Failed.Load(), ciphertextDiffers)
	}
	for i := range got {
		if !bytes.Equal(got[i], opus[i]) {
			t.Fatalf("audio frame %d differs after bridge encrypt + web decrypt", i)
		}
	}

	// Video, web → bridge, VP8 and H264: frames packetized after encryption, as the browser does.
	for _, codec := range []struct {
		mime  string
		frame func(i int) []byte
		pay   rtp.Payloader
		dep   rtp.Depacketizer
	}{
		{webrtc.MimeTypeVP8, vp8Frame, &codecs.VP8Payloader{EnablePictureID: true}, &codecs.VP8Packet{}},
		{webrtc.MimeTypeH264, h264Frame, &codecs.H264Payloader{}, &codecs.H264Packet{}},
	} {
		t.Run(strings.TrimPrefix(codec.mime, "video/"), func(t *testing.T) {
			handler := framecrypt.HandlerForCodec(false, strings.ToUpper(strings.TrimPrefix(codec.mime, "video/")))
			in := make([][]byte, 20)
			wire := newRTPPipe()
			var seq uint16
			for i := range in {
				in[i] = codec.frame(i)
				ct, err := web.e.enc.Encrypt(ctx, handler, in[i])
				if err != nil {
					t.Fatal(err)
				}
				pls := codec.pay.Payload(1188, ct)
				for j, pl := range pls {
					wire.ch <- &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 96, SSRC: 5, SequenceNumber: seq,
						Timestamp: uint32(i) * callbridge.VideoFrameTicks, Marker: j == len(pls)-1}, Payload: pl}
					seq++
				}
			}
			close(wire.ch)
			xf, closeFn, err := bridge.e.decryptor(web.id(), false, log)
			if err != nil {
				t.Fatal(err)
			}
			defer closeFn()
			out := newRTPPipe()
			var stats callbridge.FrameRelayStats
			if err := callbridge.RelayVideoTransformed(ctx, wire, 96, codec.mime, out, xf, nil, &stats, log); err != nil {
				t.Fatal(err)
			}
			close(out.ch)
			var frames [][]byte
			var cur []byte
			for p := range out.ch {
				b, err := codec.dep.Unmarshal(p.Payload)
				if err != nil {
					t.Fatal(err)
				}
				cur = append(cur, b...)
				if p.Marker {
					frames = append(frames, cur)
					cur = nil
				}
			}
			if stats.Failed.Load() != 0 {
				t.Fatalf("%d of %d frames failed to decrypt", stats.Failed.Load(), stats.Frames.Load())
			}
			// The frame assembler holds back the last frame until a later one arrives.
			if len(frames) < len(in)-1 {
				t.Fatalf("got %d of %d frames", len(frames), len(in))
			}
			for i, f := range frames {
				if !bytes.Equal(f, in[i]) {
					t.Fatalf("frame %d differs after encrypt, packetize, depacketize, decrypt", i)
				}
			}
			t.Logf("%d frames, %d packets relayed", len(frames), stats.Forwarded.Load())
		})
	}
}

// TestGroupE2eeUntrustedIdentity: when the identity store rejects a participant's identity key,
// the bridge neither sends them its key nor takes theirs, so no media decrypts either way.
func TestGroupE2eeUntrustedIdentity(t *testing.T) {
	sfu := newE2eeSFU(t)
	distrust := func(string, int32, []byte) bool { return false }
	bridge := sfu.join("100000000000001", "0a6b7c4e-2f1d-4e0a-9b3c-5d6e7f8091a2", 3, distrust)
	web := sfu.join("100000000000002", "hZvKyjdfjh6GgP1x", 7, nil)
	sfu.pushState()
	if _, _, err := bridge.e.decryptor(web.id(), true, zerolog.Nop()); err == nil {
		t.Error("the bridge made a decryptor for an untrusted participant")
	}
	// Probe below the bridge's own checks: a decryptor made in the module directly.
	fd, err := bridge.e.km.NewFrameDecryptor(context.Background(), web.id(), framecrypt.DecryptorHandlers(true))
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close(context.Background())
	webEnc := web.e.encryptTransform(zerolog.Nop())
	time.Sleep(time.Second)
	if ct, err := webEnc([]byte("probe frame")); err == nil {
		if _, err := fd.Decrypt(context.Background(), ct); err == nil {
			t.Error("the bridge installed an untrusted participant's key")
		}
	}
	if waitDecryptable(t, bridge, web, 2*time.Second) {
		t.Error("an untrusted participant got the bridge's key")
	}
}

// vp8Frame is a VP8 frame-shaped buffer (a keyframe header on the first).
func vp8Frame(i int) []byte {
	n := 300 + 1700*(i%3)
	f := make([]byte, n)
	_, _ = rand.Read(f)
	if i == 0 {
		f[0] = 0x10 // keyframe, show_frame
		copy(f[3:], []byte{0x9d, 0x01, 0x2a})
	} else {
		f[0] |= 0x01
	}
	return f
}

// h264Frame is an Annex B access unit: SPS and PPS on the first, then one slice.
func h264Frame(i int) []byte {
	var f []byte
	if i == 0 {
		f = append(f, 0, 0, 0, 1, 0x67, 0x42, 0xc0, 0x1f, 0xda, 0x01, 0x40, 0x16, 0xe8)
		f = append(f, 0, 0, 0, 1, 0x68, 0xce, 0x3c, 0x80)
	}
	nal := byte(0x41)
	if i == 0 {
		nal = 0x65
	}
	slice := make([]byte, 400+1500*(i%3))
	_, _ = rand.Read(slice)
	// No start-code emulation inside the slice.
	for j := range slice {
		if slice[j] == 0 {
			slice[j] = 1
		}
	}
	f = append(f, 0, 0, 0, 1, nal)
	return append(f, slice...)
}

func loaderTestClient(t *testing.T, module string) *MetaClient {
	log := zerolog.New(zerolog.NewTestWriter(t))
	mc := &MetaConnector{Bridge: &bridgev2.Bridge{Log: log}}
	mc.Config.CallBridgingFrameEncryptionModule = module
	return &MetaClient{Main: mc, Client: messagix.NewClient(&cookies.Cookies{Platform: types.Facebook}, log, &messagix.Config{})}
}

// TestFrameCryptLoaderFile loads the module from a configured file, without going to the network.
func TestFrameCryptLoaderFile(t *testing.T) {
	path := os.Getenv("FRAMECRYPT_WASM")
	if path == "" {
		t.Skip("FRAMECRYPT_WASM not set")
	}
	m := loaderTestClient(t, path)
	rt, err := m.frameCryptRuntime(context.Background(), "1")
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := m.frameCryptRuntime(context.Background(), "1"); again != rt {
		t.Error("the module was loaded twice")
	}
}

// TestFrameCryptLoaderLive loads the module like the bridge does in production (FRAMECRYPT_LIVE=1;
// fetches from Meta). Logged out, the group-call page has no bxData, so this takes the fallback URL.
func TestFrameCryptLoaderLive(t *testing.T) {
	if os.Getenv("FRAMECRYPT_LIVE") == "" {
		t.Skip("FRAMECRYPT_LIVE not set")
	}
	m := loaderTestClient(t, "")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	rt, err := m.frameCryptRuntime(ctx, "1")
	if err != nil {
		t.Fatal(err)
	}
	in, err := rt.NewInstance(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = in.Close(ctx)
}
