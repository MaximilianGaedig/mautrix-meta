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

package callbridge

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/rs/zerolog"
	"go.mau.fi/libsignal/ecc"

	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
)

// loopbackSettings keeps ICE on 127.0.0.1 without mDNS, so tests need no
// network.
func loopbackSettings() *webrtc.SettingEngine {
	se := &webrtc.SettingEngine{}
	se.SetIncludeLoopbackCandidate(true)
	se.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	se.SetInterfaceFilter(func(name string) bool { return name == "lo" || name == "lo0" })
	return se
}

func newTestLeg(t *testing.T, name string, pt uint8, webShape bool) *Leg {
	t.Helper()
	l, err := NewLeg(LegConfig{Name: name, OpusPT: pt, WebShape: webShape, Settings: loopbackSettings(), Log: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Close)
	return l
}

func testIdentity(t *testing.T) *Identity {
	t.Helper()
	kp, err := ecc.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	return &Identity{
		UserID:   100000000000002,
		DeviceID: 7,
		Priv:     kp.PrivateKey().Serialize(),
		Pub:      kp.PublicKey().PublicKey(),
	}
}

// mLines returns "kind direction" per m-line of an SDP.
func mLines(sdp string) []string {
	var out []string
	for _, sec := range strings.Split(sdp, "\r\nm=")[1:] {
		kind, _, _ := strings.Cut(sec, " ")
		sec += "\r\n"
		dir := ""
		if kind == "application" {
			out = append(out, kind)
			continue
		}
		for _, d := range []string{"sendrecv", "sendonly", "recvonly", "inactive"} {
			if strings.Contains(sec, "\r\na="+d+"\r\n") {
				dir = d
			}
		}
		out = append(out, strings.TrimSpace(kind+" "+dir))
	}
	return out
}

// TestWebShapedSDP checks the SDP the bridge sends to Messenger, both as
// callee (answering a web-shaped offer) and as caller (offering): audio +
// video + application m-lines in one BUNDLE, Opus 111, upper-case
// fingerprint, the web ice-options, and a valid x-dtls-auth.
func TestWebShapedSDP(t *testing.T) {
	id := testIdentity(t)

	web := newTestLeg(t, "web", OpusPT, true)
	webOffer, err := web.CreateOffer()
	if err != nil {
		t.Fatal(err)
	}
	if got := mLines(webOffer); fmt.Sprint(got) != "[audio sendrecv video recvonly application]" {
		t.Fatalf("offer m-lines: %v", got)
	}

	meta := newTestLeg(t, "meta", OpusPT, true)
	answer, err := meta.AnswerOffer(PrepareMetaRemoteSDP(webOffer))
	if err != nil {
		t.Fatal(err)
	}
	signed, err := PrepareMetaLocalSDP(answer, id, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := mLines(signed); fmt.Sprint(got) != "[audio sendrecv video inactive application]" {
		t.Errorf("answer m-lines: %v", got)
	}
	for _, want := range []string{"a=group:BUNDLE 0 1 2", "a=rtpmap:111 opus/48000/2", webICEOptions, "a=msid:" + meta.StreamID + " " + meta.TrackID} {
		if !strings.Contains(signed, want+"\r\n") {
			t.Errorf("answer lacks %q", want)
		}
	}
	fps := rtcsignal.SDPFingerprints(signed)
	if len(fps) != 1 {
		t.Fatalf("want one distinct fingerprint, got %d", len(fps))
	}
	if algo, digest, _ := strings.Cut(fps[0], " "); algo != "sha-256" || digest != strings.ToUpper(digest) {
		t.Errorf("fingerprint not sha-256 with an upper-case digest")
	}
	info, err := rtcsignal.VerifyDTLSAuth(signed, id.UserID, append([]byte{ecc.DjbType}, id.Pub[:]...))
	if err != nil {
		t.Fatalf("x-dtls-auth does not verify: %v", err)
	}
	if info.DeviceID != id.DeviceID {
		t.Errorf("device id %d", info.DeviceID)
	}
	if _, err = rtcsignal.VerifyDTLSAuth(signed, id.UserID+1, nil); err == nil {
		t.Error("x-dtls-auth verifies for another user")
	}
	if AudioTrackID(signed) != meta.TrackID {
		t.Errorf("AudioTrackID = %q", AudioTrackID(signed))
	}
	// The web side accepts the answer once x-dtls-auth is stripped.
	if err = web.SetAnswer(PrepareMetaRemoteSDP(signed)); err != nil {
		t.Fatalf("web peer rejects the answer: %v", err)
	}

	// As caller the bridge offers the same shape.
	caller := newTestLeg(t, "meta-caller", OpusPT, true)
	offer, err := caller.CreateOffer()
	if err != nil {
		t.Fatal(err)
	}
	offer, err = PrepareMetaLocalSDP(offer, id, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := mLines(offer); fmt.Sprint(got) != "[audio sendrecv video recvonly application]" {
		t.Errorf("caller offer m-lines: %v", got)
	}
	if _, err = rtcsignal.VerifyDTLSAuth(offer, id.UserID, nil); err != nil {
		t.Errorf("caller offer x-dtls-auth: %v", err)
	}
}

func TestRewriterSourceSwitch(t *testing.T) {
	var rw Rewriter
	pkt := func(ssrc uint32, seq uint16, ts uint32) *rtp.Packet {
		p := &rtp.Packet{Header: rtp.Header{SSRC: ssrc, SequenceNumber: seq, Timestamp: ts}}
		_ = p.Header.SetExtension(1, []byte{0x80})
		return p
	}
	p1 := pkt(1, 100, 5000)
	rw.Rewrite(p1)
	p2 := pkt(1, 101, 5960)
	rw.Rewrite(p2)
	if p2.SequenceNumber != 101 || p2.Timestamp != 5960 || p2.Extension || p2.Extensions != nil {
		t.Fatalf("same-source packet changed: %+v", p2.Header)
	}
	p3 := pkt(2, 60000, 123)
	rw.Rewrite(p3)
	if p3.SequenceNumber != 102 || p3.Timestamp != 5960+opusFrameTicks {
		t.Fatalf("switched source not re-based: seq %d ts %d", p3.SequenceNumber, p3.Timestamp)
	}
	p4 := pkt(2, 60001, 123+960)
	rw.Rewrite(p4)
	if p4.SequenceNumber != 103 || p4.Timestamp != 5960+2*opusFrameTicks {
		t.Fatalf("continuation wrong: seq %d ts %d", p4.SequenceNumber, p4.Timestamp)
	}
}

// connect runs a full offer/answer (no trickle) between an offerer and an
// answerer leg.
func connect(t *testing.T, ctx context.Context, offerer, answerer *Leg) {
	t.Helper()
	if _, err := offerer.CreateOffer(); err != nil {
		t.Fatal(err)
	}
	offer := offerer.WaitGathering(ctx, 5*time.Second)
	if _, err := answerer.AnswerOffer(offer); err != nil {
		t.Fatal(err)
	}
	answer := answerer.WaitGathering(ctx, 5*time.Second)
	if err := offerer.SetAnswer(answer); err != nil {
		t.Fatal(err)
	}
}

// TestLoopbackRelay connects two local peers through the bridge's two legs
// and the relay, with different Opus payload types on each side, and checks
// that Opus payloads arrive intact in both directions with the bridge's own
// SSRCs and the receiving leg's payload type.
func TestLoopbackRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	log := zerolog.Nop()

	// Messenger side: peer A offers with PT 111, the bridge's meta leg answers.
	peerA := newTestLeg(t, "peerA", 111, true)
	metaLeg := newTestLeg(t, "meta", 111, true)
	// Matrix side: the bridge's matrix leg offers with PT 109, peer B answers.
	mxLeg := newTestLeg(t, "matrix", 109, false)
	peerB := newTestLeg(t, "peerB", 109, false)

	connect(t, ctx, peerA, metaLeg)
	connect(t, ctx, mxLeg, peerB)

	// Both peers send; the bridge relays whatever arrives.
	send := func(l *Leg, tag string) {
		go func() {
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			for i := uint16(0); ; i++ {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				_ = l.Local.WriteRTP(&rtp.Packet{
					Header:  rtp.Header{Version: 2, SequenceNumber: 1000 + i, Timestamp: uint32(i) * 960, Marker: i == 0},
					Payload: fmt.Appendf(nil, "%s-opus-%d", tag, i),
				})
			}
		}()
	}
	send(peerA, "A")
	send(peerB, "B")

	var statsAB, statsBA RelayStats
	go func() {
		tr, err := metaLeg.RemoteTrack(ctx)
		if err != nil {
			return
		}
		_ = Relay(ctx, tr, uint8(tr.PayloadType()), mxLeg.Local, &statsAB, log)
	}()
	go func() {
		tr, err := mxLeg.RemoteTrack(ctx)
		if err != nil {
			return
		}
		_ = Relay(ctx, tr, uint8(tr.PayloadType()), metaLeg.Local, &statsBA, log)
	}()

	expect := func(receiver *Leg, wantPT uint8, tag string, notSSRC uint32) {
		t.Helper()
		tr, err := receiver.RemoteTrack(ctx)
		if err != nil {
			t.Fatalf("%s: %v", receiver.Name, err)
		}
		got := 0
		for got < 10 {
			p, _, err := tr.ReadRTP()
			if err != nil {
				t.Fatalf("%s: read: %v", receiver.Name, err)
			}
			if p.PayloadType != wantPT {
				t.Fatalf("%s: payload type %d, want %d", receiver.Name, p.PayloadType, wantPT)
			}
			if p.SSRC == notSSRC {
				t.Fatalf("%s: SSRC was not remapped", receiver.Name)
			}
			if !bytes.HasPrefix(p.Payload, []byte(tag+"-opus-")) {
				t.Fatalf("%s: unexpected payload %q", receiver.Name, p.Payload)
			}
			got++
		}
	}
	senderSSRC := func(l *Leg) uint32 {
		for _, s := range l.PC.GetSenders() {
			if s.Track() != nil {
				return uint32(s.GetParameters().Encodings[0].SSRC)
			}
		}
		return 0
	}
	expect(peerB, 109, "A", senderSSRC(peerA))
	expect(peerA, 111, "B", senderSSRC(peerB))
	if statsAB.Forwarded.Load() == 0 || statsBA.Forwarded.Load() == 0 {
		t.Fatalf("relay stats: A->B %d, B->A %d", statsAB.Forwarded.Load(), statsBA.Forwarded.Load())
	}
}

func newVideoTestLeg(t *testing.T, name string, webShape bool) *Leg {
	t.Helper()
	l, err := NewLeg(LegConfig{
		Name: name, WebShape: webShape, VideoCodec: webrtc.MimeTypeVP8, Settings: loopbackSettings(), Log: zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Close)
	return l
}

// TestLoopbackVideoRelay relays VP8 both ways through the two legs and checks
// that a keyframe request from one peer reaches the leg facing the other.
func TestLoopbackVideoRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	log := zerolog.Nop()
	peerA := newVideoTestLeg(t, "peerA", true)
	metaLeg := newVideoTestLeg(t, "meta", true)
	mxLeg := newVideoTestLeg(t, "matrix", false)
	peerB := newVideoTestLeg(t, "peerB", false)
	connect(t, ctx, peerA, metaLeg)
	connect(t, ctx, mxLeg, peerB)
	if !SendsVideo(metaLeg.PC.LocalDescription().SDP) || !SendsVideo(mxLeg.PC.LocalDescription().SDP) {
		t.Fatal("bridge legs don't send video")
	}

	send := func(l *Leg, tag string) {
		go func() {
			ticker := time.NewTicker(33 * time.Millisecond)
			defer ticker.Stop()
			for i := uint16(0); ; i++ {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				_ = l.LocalVideo.WriteRTP(&rtp.Packet{
					Header:  rtp.Header{Version: 2, SequenceNumber: 500 + i, Timestamp: uint32(i) * VideoFrameTicks, Marker: true},
					Payload: fmt.Appendf(nil, "%s-vp8-%d", tag, i),
				})
			}
		}()
	}
	send(peerA, "A")
	send(peerB, "B")
	keyframeRequested := make(chan struct{}, 1)
	mxLeg.OnKeyframeRequest(func() {
		select {
		case keyframeRequested <- struct{}{}:
		default:
		}
	})
	for _, dir := range [][2]*Leg{{metaLeg, mxLeg}, {mxLeg, metaLeg}} {
		go func() {
			tr, err := dir[0].RemoteVideoTrack(ctx)
			if err != nil {
				return
			}
			var stats RelayStats
			_ = RelayVideo(ctx, tr, uint8(tr.PayloadType()), dir[1].LocalVideo, &stats, log)
		}()
	}
	expect := func(receiver *Leg, tag string) *webrtc.TrackRemote {
		t.Helper()
		tr, err := receiver.RemoteVideoTrack(ctx)
		if err != nil {
			t.Fatalf("%s: %v", receiver.Name, err)
		}
		if tr.Codec().MimeType != webrtc.MimeTypeVP8 {
			t.Fatalf("%s: codec %s", receiver.Name, tr.Codec().MimeType)
		}
		for got := 0; got < 5; {
			p, _, err := tr.ReadRTP()
			if err != nil {
				t.Fatalf("%s: read: %v", receiver.Name, err)
			}
			if bytes.HasPrefix(p.Payload, []byte(tag+"-vp8-")) {
				got++
			}
		}
		return tr
	}
	trB := expect(peerB, "A")
	expect(peerA, "B")
	peerB.RequestKeyframe(trB.SSRC())
	select {
	case <-keyframeRequested:
	case <-ctx.Done():
		t.Fatal("keyframe request from peer B didn't reach the matrix leg")
	}
}

// TestPlanBOffer answers a Plan B offer, as Messenger's mobile apps send.
func TestPlanBOffer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	api := webrtc.NewAPI(webrtc.WithSettingEngine(*loopbackSettings()))
	peer, err := api.NewPeerConnection(webrtc.Configuration{SDPSemantics: webrtc.SDPSemanticsPlanB})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}, "a", "s")
	if err != nil {
		t.Fatal(err)
	}
	// Two tracks in one m-section is what makes an offer (certainly) Plan B.
	track2, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}, "b", "s")
	if err != nil {
		t.Fatal(err)
	}
	for _, tr := range []webrtc.TrackLocal{track, track2} {
		if _, err = peer.AddTrack(tr); err != nil {
			t.Fatal(err)
		}
	}
	offer, err := peer.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gathered := webrtc.GatheringCompletePromise(peer)
	if err = peer.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	<-gathered
	if !strings.Contains(peer.LocalDescription().SDP, "a=ssrc:") {
		t.Fatal("test offer isn't Plan B")
	}
	leg := newTestLeg(t, "meta", 111, true)
	if _, err = leg.AnswerOffer(peer.LocalDescription().SDP); err != nil {
		t.Fatalf("answering a Plan B offer: %v", err)
	}
	answer := leg.WaitGathering(ctx, 5*time.Second)
	if err = peer.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}); err != nil {
		t.Fatal(err)
	}
	go func() {
		for i := uint16(0); ctx.Err() == nil; i++ {
			_ = track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, SequenceNumber: i, Timestamp: uint32(i) * 960}, Payload: []byte("planb")})
			time.Sleep(20 * time.Millisecond)
		}
	}()
	tr, err := leg.RemoteTrack(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if p, _, err := tr.ReadRTP(); err != nil || string(p.Payload) != "planb" {
		t.Fatalf("read: %v %v", p, err)
	}
}

func TestVideoSDPHelpers(t *testing.T) {
	web := "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=rtpmap:111 opus/48000/2\r\na=sendrecv\r\n" +
		"m=video 9 UDP/TLS/RTP/SAVPF 108 96\r\na=rtpmap:108 H264/90000\r\na=rtpmap:96 VP8/90000\r\na=sendrecv\r\n"
	if PickVideoCodec(web) != webrtc.MimeTypeVP8 || !SendsVideo(web) {
		t.Fatal("web video offer")
	}
	h264 := strings.Replace(web, "a=rtpmap:96 VP8/90000\r\n", "", 1)
	if PickVideoCodec(h264) != webrtc.MimeTypeH264 {
		t.Fatal("H264-only offer")
	}
	voice := strings.TrimSuffix(web, "a=sendrecv\r\n") + "a=recvonly\r\n"
	if SendsVideo(voice) {
		t.Fatal("recvonly video counted as sending")
	}
	if SendsVideo("v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n") || PickVideoCodec("m=video 0 RTP 96\r\na=rtpmap:96 VP8/90000\r\n") != "" {
		t.Fatal("audio-only / disabled video")
	}
}

func TestLogSDPShapeHasNoSecrets(t *testing.T) {
	sdp := "v=0\r\no=- 1 2 IN IP4 203.0.113.9\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\nc=IN IP4 203.0.113.9\r\n" +
		"a=ice-ufrag:SECRETUFRAG\r\na=ice-pwd:SECRETPWD\r\na=fingerprint:sha-256 AB:CD\r\na=x-dtls-auth:SECRETAUTH\r\n" +
		"a=candidate:1 1 udp 1 203.0.113.9 5000 typ host\r\na=mid:0\r\na=sendrecv\r\na=rtpmap:111 opus/48000/2\r\n" +
		"a=ssrc:1 cname:x\r\na=ssrc:2 cname:x\r\na=extmap:1 urn:ietf:params:rtp-hdrext:sdes:mid\r\n" +
		"m=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:1\r\na=recvonly\r\na=rtpmap:96 VP8/90000\r\n"
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	LogSDPShape(log.Info(), sdp).Msg("x")
	out := buf.String()
	for _, secret := range []string{"SECRET", "203.0.113", "AB:CD"} {
		if strings.Contains(out, secret) {
			t.Fatalf("shape log leaks %q: %s", secret, out)
		}
	}
	for _, want := range []string{`"ssrcs":2`, `"dir":"recvonly"`, "VP8/90000", "sdes:mid"} {
		if !strings.Contains(out, want) {
			t.Fatalf("shape log misses %q: %s", want, out)
		}
	}
}
