package connector

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2/callbridge"
)

// twoPayloadTypes stands for a Messenger leg that agreed the track's codec under 97 and 126.
type twoPayloadTypes struct{ asked int }

func (l *twoPayloadTypes) VideoPayloadTypes(*webrtc.TrackRemote) []uint8 {
	l.asked++
	return []uint8{97, 126}
}

// A Messenger phone agrees H264 under two payload types and moves a track between them (a screen
// shared in the camera's track). The group call and the legacy 1:1 relay took only the one the
// track began with, so the picture froze at the move.
func TestRelayLegVideoTakesEveryAgreedPayloadType(t *testing.T) {
	src := newRTPPipe()
	for seq, pt := range []uint8{97, 97, 126, 126, 98, 97} {
		src.ch <- &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: pt, SSRC: 5, SequenceNumber: uint16(seq)}, Payload: []byte{byte(seq)}}
	}
	close(src.ch)
	dst := newRTPPipe()
	leg := &twoPayloadTypes{}
	var stats callbridge.RelayStats
	if err := relayLegVideo(context.Background(), leg, nil, src, dst, &stats, zerolog.Nop()); err != nil {
		t.Fatal(err)
	}
	close(dst.ch)
	var got []byte
	for p := range dst.ch {
		got = append(got, p.Payload[0])
	}
	// Everything but the retransmission under 98, in order.
	if want := []byte{0, 1, 2, 3, 5}; !bytes.Equal(got, want) || stats.Dropped.Load() != 1 || leg.asked != 1 {
		t.Fatalf("relayed packets %v (want %v), dropped %d (want 1), leg asked %d times", got, want, stats.Dropped.Load(), leg.asked)
	}
}

// The same in an encrypted group call, where frames are taken apart, decrypted and packetized again.
func TestRelayLegVideoTransformedTakesEveryAgreedPayloadType(t *testing.T) {
	const frames = 12
	src := newRTPPipe()
	pay := &codecs.H264Payloader{}
	var in [][]byte
	var seq uint16
	for i := range frames {
		in = append(in, h264Frame(i))
		// The track moves to the second payload type halfway.
		pt := uint8(97)
		if i >= frames/2 {
			pt = 126
		}
		pls := pay.Payload(1188, in[i])
		for j, pl := range pls {
			src.ch <- &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: pt, SSRC: 5, SequenceNumber: seq,
				Timestamp: uint32(i) * callbridge.VideoFrameTicks, Marker: j == len(pls)-1}, Payload: pl}
			seq++
		}
	}
	close(src.ch)
	dst := newRTPPipe()
	var stats callbridge.FrameRelayStats
	same := func(frame []byte) ([]byte, error) { return frame, nil }
	err := relayLegVideoTransformed(context.Background(), &twoPayloadTypes{}, nil, src, webrtc.MimeTypeH264, dst, same, nil, &stats, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	close(dst.ch)
	var got [][]byte
	var cur []byte
	dep := &codecs.H264Packet{}
	for p := range dst.ch {
		b, err := dep.Unmarshal(p.Payload)
		if err != nil {
			t.Fatal(err)
		}
		cur = append(cur, b...)
		if p.Marker {
			got = append(got, cur)
			cur = nil
		}
	}
	// The frame assembler holds back the last frame until a later one arrives.
	if len(got) < frames-1 || stats.Dropped.Load() != 0 {
		t.Fatalf("got %d of %d frames, %d packets dropped", len(got), frames, stats.Dropped.Load())
	}
	for i, f := range got {
		if !bytes.Equal(f, in[i]) {
			t.Fatalf("frame %d differs", i)
		}
	}
}

func TestPayloadTypeFolderLeavesOtherPayloadTypes(t *testing.T) {
	src := newRTPPipe()
	for _, pt := range []uint8{126, 98, 97} {
		src.ch <- &rtp.Packet{Header: rtp.Header{PayloadType: pt}}
	}
	close(src.ch)
	f := &payloadTypeFolder{src: src, pts: []uint8{97, 126}}
	var got []uint8
	for {
		p, _, err := f.ReadRTP()
		if err != nil {
			break
		}
		got = append(got, p.PayloadType)
	}
	if want := []uint8{97, 98, 97}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
