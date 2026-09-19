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

// Package callbridge holds the media side of Messenger <-> Matrix call
// bridging: one Pion PeerConnection per side ("leg") and an RTP relay that
// forwards Opus between them without transcoding.
package callbridge

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/rs/zerolog"
)

// OpusPT is the Opus payload type both Chrome and Element use.
const OpusPT = 111

// Opus fmtp offered on the bridge's legs; answers adopt the offerer's.
const webOpusFmtp = "minptime=10;useinbandfec=1"

// LegConfig configures one PeerConnection.
type LegConfig struct {
	Name       string
	ICEServers []webrtc.ICEServer
	// OpusPT is the payload type Opus is registered under when this leg
	// makes the offer. Answers use the offerer's payload type.
	OpusPT uint8
	// WebShape registers the web client's video codecs and adds a recvonly
	// video transceiver and a data channel to offers, so an offer has the
	// same m-lines (audio, video, application) as facebook.com's.
	WebShape bool
	// VideoCodec is the video codec (webrtc.MimeTypeVP8 or MimeTypeH264)
	// this leg sends and receives, or "" for an audio call. Both legs of a
	// call use the same one, so video is relayed without transcoding. Only
	// that codec is registered, which forces the negotiation.
	VideoCodec string
	// Settings optionally overrides the Pion setting engine (tests use it to
	// restrict ICE to loopback).
	Settings *webrtc.SettingEngine
	Log      zerolog.Logger
}

// Leg is one side of a bridged call.
type Leg struct {
	Name  string
	PC    *webrtc.PeerConnection
	Local *webrtc.TrackLocalStaticRTP
	// TrackID and StreamID are the msid of Local.
	TrackID, StreamID string
	// LocalVideo is the video track relayed to this leg (nil for audio
	// calls), VideoTrackID its msid track id.
	LocalVideo   *webrtc.TrackLocalStaticRTP
	VideoTrackID string

	log     zerolog.Logger
	cfg     LegConfig
	sender  *webrtc.RTPSender
	vsender *webrtc.RTPSender
	remote  chan *webrtc.TrackRemote
	remoteV chan *webrtc.TrackRemote

	onKeyframeRequest func()
	lock              sync.Mutex
	pending           []webrtc.ICECandidateInit
	haveDesc          bool
	closed            bool

	onCandidate func(*webrtc.ICECandidateInit)
	onState     func(webrtc.PeerConnectionState)
	gathered    chan struct{}
}

// NewLeg creates a PeerConnection with Opus audio (and, for WebShape, the
// web client's video codecs).
func NewLeg(cfg LegConfig) (*Leg, error) {
	if cfg.OpusPT == 0 {
		cfg.OpusPT = OpusPT
	}
	me := &webrtc.MediaEngine{}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2, SDPFmtpLine: webOpusFmtp,
		},
		PayloadType: webrtc.PayloadType(cfg.OpusPT),
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, err
	}
	if cfg.WebShape || cfg.VideoCodec != "" {
		for _, c := range videoCodecs {
			if cfg.VideoCodec != "" && c.MimeType != cfg.VideoCodec {
				continue
			}
			if err := me.RegisterCodec(c, webrtc.RTPCodecTypeVideo); err != nil {
				return nil, err
			}
		}
	}
	ir := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(me, ir); err != nil {
		return nil, err
	}
	var se webrtc.SettingEngine
	if cfg.Settings != nil {
		se = *cfg.Settings
	}
	// Pion holds back nominating a working pair for its candidate type's
	// "acceptance min wait": 2 s for relay and 1 s for peer-reflexive by
	// default, which was most of the time between answering and hearing the
	// other side. Browsers nominate as soon as a check succeeds; keep only a
	// short head start for the better (direct) pairs.
	se.SetHostAcceptanceMinWait(0)
	se.SetSrflxAcceptanceMinWait(50 * time.Millisecond)
	se.SetPrflxAcceptanceMinWait(100 * time.Millisecond)
	se.SetRelayAcceptanceMinWait(200 * time.Millisecond)
	// Pion's DTLS retransmits after 1 s; a lost first flight cost a second
	// of silence after ICE connected.
	se.SetDTLSRetransmissionInterval(100 * time.Millisecond)
	// Pion's own warnings (e.g. RTP for an SSRC no transceiver claims, so
	// no track ever starts) go to the call log.
	se.LoggerFactory = &pionLoggerFactory{log: cfg.Log.With().Str("leg", cfg.Name).Logger()}
	opts := []func(*webrtc.API){
		webrtc.WithMediaEngine(me), webrtc.WithInterceptorRegistry(ir), webrtc.WithSettingEngine(se),
	}
	api := webrtc.NewAPI(opts...)
	pc, err := api.NewPeerConnection(webrtc.Configuration{
		ICEServers: cfg.ICEServers,
		// Messenger's mobile apps still offer Plan B SDP (one m-line per
		// kind, tracks as a=ssrc groups); the web client and Element use
		// Unified Plan. Answer whichever the peer offers.
		SDPSemantics:  webrtc.SDPSemanticsUnifiedPlanWithFallback,
		BundlePolicy:  webrtc.BundlePolicyMaxBundle,
		RTCPMuxPolicy: webrtc.RTCPMuxPolicyRequire,
	})
	if err != nil {
		return nil, err
	}
	l := &Leg{
		Name:     cfg.Name,
		PC:       pc,
		TrackID:  uuid.NewString(),
		StreamID: uuid.NewString(),
		log:      cfg.Log.With().Str("leg", cfg.Name).Logger(),
		cfg:      cfg,
		remote:   make(chan *webrtc.TrackRemote, 1),
		remoteV:  make(chan *webrtc.TrackRemote, 1),
		gathered: make(chan struct{}),
	}
	l.Local, err = webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{
		MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2,
	}, l.TrackID, l.StreamID)
	if err != nil {
		_ = pc.Close()
		return nil, err
	}
	if cfg.VideoCodec != "" {
		vcap := videoCapability(cfg.VideoCodec)
		l.VideoTrackID = uuid.NewString()
		l.LocalVideo, err = webrtc.NewTrackLocalStaticRTP(vcap, l.VideoTrackID, l.StreamID)
		if err != nil {
			_ = pc.Close()
			return nil, err
		}
	}
	var gatherOnce sync.Once
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			gatherOnce.Do(func() { close(l.gathered) })
			l.log.Debug().Msg("ICE gathering complete")
			return
		}
		// Only the candidate type is logged; addresses stay out of the logs.
		l.log.Debug().Stringer("cand_type", c.Typ).Stringer("protocol", c.Protocol).Msg("Local ICE candidate")
		init := c.ToJSON()
		l.lock.Lock()
		cb := l.onCandidate
		l.lock.Unlock()
		if cb != nil {
			cb(&init)
		}
	})
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		l.log.Info().Stringer("state", s).Msg("PeerConnection state changed")
		l.lock.Lock()
		cb := l.onState
		l.lock.Unlock()
		if cb != nil {
			cb(s)
		}
	})
	pc.OnICEConnectionStateChange(func(s webrtc.ICEConnectionState) {
		l.log.Debug().Stringer("ice_state", s).Msg("ICE connection state changed")
	})
	pc.OnTrack(func(tr *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		l.log.Info().
			Str("kind", tr.Kind().String()).
			Str("codec", tr.Codec().MimeType).
			Uint8("pt", uint8(tr.PayloadType())).
			Msg("Remote track started")
		ch := l.remote
		if tr.Kind() == webrtc.RTPCodecTypeVideo {
			ch = l.remoteV
		}
		select {
		case ch <- tr:
		default:
		}
	})
	return l, nil
}

// OnCandidate sets the callback for local ICE candidates (trickle).
func (l *Leg) OnCandidate(fn func(*webrtc.ICECandidateInit)) {
	l.lock.Lock()
	l.onCandidate = fn
	l.lock.Unlock()
}

// OnState sets the callback for PeerConnection state changes.
func (l *Leg) OnState(fn func(webrtc.PeerConnectionState)) {
	l.lock.Lock()
	l.onState = fn
	l.lock.Unlock()
}

func (l *Leg) addLocalTrack() error {
	if l.sender != nil {
		return nil
	}
	sender, err := l.PC.AddTrack(l.Local)
	if err != nil {
		return err
	}
	l.sender = sender
	// Drain RTCP so the interceptors (reports, NACK) keep working.
	go func() {
		buf := make([]byte, 1500)
		for {
			if _, _, err := sender.Read(buf); err != nil {
				return
			}
		}
	}()
	if l.LocalVideo != nil {
		vsender, err := l.PC.AddTrack(l.LocalVideo)
		if err != nil {
			return err
		}
		l.vsender = vsender
		go l.readVideoRTCP(vsender)
	}
	return nil
}

// readVideoRTCP drains the video sender's RTCP and reports the receiver's
// keyframe requests (PLI/FIR), which the relay passes on to the sending peer.
func (l *Leg) readVideoRTCP(sender *webrtc.RTPSender) {
	for {
		pkts, _, err := sender.ReadRTCP()
		if err != nil {
			return
		}
		for _, p := range pkts {
			switch p.(type) {
			case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
				l.lock.Lock()
				cb := l.onKeyframeRequest
				l.lock.Unlock()
				if cb != nil {
					cb()
				}
			}
		}
	}
}

// OnKeyframeRequest sets the callback for keyframe requests from this leg's
// peer about the video we send it.
func (l *Leg) OnKeyframeRequest(fn func()) {
	l.lock.Lock()
	l.onKeyframeRequest = fn
	l.lock.Unlock()
}

// RequestKeyframe asks this leg's peer for a keyframe of its video.
func (l *Leg) RequestKeyframe(ssrc webrtc.SSRC) {
	_ = l.PC.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(ssrc)}})
}

// RemoteVideoTrack waits for the peer's video track.
func (l *Leg) RemoteVideoTrack(ctx context.Context) (*webrtc.TrackRemote, error) {
	select {
	case tr := <-l.remoteV:
		return tr, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("callbridge: no remote video track: %w", ctx.Err())
	}
}

var videoFeedback = []webrtc.RTCPFeedback{
	{Type: "goog-remb"}, {Type: "ccm", Parameter: "fir"}, {Type: "nack"}, {Type: "nack", Parameter: "pli"},
}

// videoCodecs are the web client's video codecs (H264 first, as it offers
// them), with its payload types.
var videoCodecs = []webrtc.RTPCodecParameters{
	{RTPCodecCapability: videoCapability(webrtc.MimeTypeH264), PayloadType: 108},
	{RTPCodecCapability: videoCapability(webrtc.MimeTypeVP8), PayloadType: 96},
}

func videoCapability(mime string) webrtc.RTPCodecCapability {
	c := webrtc.RTPCodecCapability{MimeType: mime, ClockRate: 90000, RTCPFeedback: videoFeedback}
	if mime == webrtc.MimeTypeH264 {
		c.SDPFmtpLine = "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"
	}
	return c
}

// CreateOffer makes and applies a local offer: audio sendrecv, plus for
// WebShape a recvonly video m-line and a data channel, in that order.
func (l *Leg) CreateOffer() (string, error) {
	if err := l.addLocalTrack(); err != nil {
		return "", err
	}
	if l.cfg.WebShape {
		if l.LocalVideo == nil {
			if _, err := l.PC.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{
				Direction: webrtc.RTPTransceiverDirectionRecvonly,
			}); err != nil {
				return "", err
			}
		}
		if _, err := l.PC.CreateDataChannel("signaling_channel", nil); err != nil {
			return "", err
		}
	}
	offer, err := l.PC.CreateOffer(nil)
	if err != nil {
		return "", err
	}
	if err = l.PC.SetLocalDescription(offer); err != nil {
		return "", err
	}
	return l.PC.LocalDescription().SDP, nil
}

// AnswerOffer applies a remote offer and makes the local answer. Pion answers
// every offered m-line: audio sendrecv with our track, a recvonly video
// offer inactive, and the data channel.
func (l *Leg) AnswerOffer(offer string) (string, error) {
	if err := l.setRemote(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer}); err != nil {
		return "", fmt.Errorf("set remote offer: %w", err)
	}
	if err := l.addLocalTrack(); err != nil {
		return "", err
	}
	answer, err := l.PC.CreateAnswer(nil)
	if err != nil {
		return "", err
	}
	if err = l.PC.SetLocalDescription(answer); err != nil {
		return "", err
	}
	return l.PC.LocalDescription().SDP, nil
}

// SetAnswer applies the remote answer to our offer.
func (l *Leg) SetAnswer(answer string) error {
	return l.setRemote(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer})
}

func (l *Leg) setRemote(desc webrtc.SessionDescription) error {
	if err := l.PC.SetRemoteDescription(desc); err != nil {
		return err
	}
	l.lock.Lock()
	l.haveDesc = true
	pending := l.pending
	l.pending = nil
	l.lock.Unlock()
	for _, c := range pending {
		if err := l.PC.AddICECandidate(c); err != nil {
			l.log.Debug().Err(err).Msg("Failed to add queued remote ICE candidate")
		}
	}
	return nil
}

// AddCandidate adds a remote candidate, queueing it until the remote
// description is known. An empty candidate (end of candidates) is ignored.
func (l *Leg) AddCandidate(c webrtc.ICECandidateInit) error {
	if c.Candidate == "" {
		return nil
	}
	l.lock.Lock()
	if !l.haveDesc {
		l.pending = append(l.pending, c)
		l.lock.Unlock()
		return nil
	}
	l.lock.Unlock()
	return l.PC.AddICECandidate(c)
}

// WaitGathering waits until ICE gathering finishes or the timeout passes and
// returns the current local SDP (with the candidates gathered so far).
func (l *Leg) WaitGathering(ctx context.Context, timeout time.Duration) string {
	select {
	case <-l.gathered:
	case <-time.After(timeout):
	case <-ctx.Done():
	}
	if d := l.PC.LocalDescription(); d != nil {
		return d.SDP
	}
	return ""
}

// ErrNoRemoteTrack is returned when the peer never started sending audio.
var ErrNoRemoteTrack = errors.New("callbridge: no remote audio track")

// RemoteTrack waits for the remote audio track.
func (l *Leg) RemoteTrack(ctx context.Context) (*webrtc.TrackRemote, error) {
	select {
	case tr := <-l.remote:
		return tr, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %w", ErrNoRemoteTrack, ctx.Err())
	}
}

// Close closes the PeerConnection. It is safe to call more than once.
func (l *Leg) Close() {
	if l == nil {
		return
	}
	l.lock.Lock()
	if l.closed {
		l.lock.Unlock()
		return
	}
	l.closed = true
	l.onCandidate = nil
	l.onState = nil
	l.lock.Unlock()
	if err := l.PC.Close(); err != nil {
		l.log.Debug().Err(err).Msg("Error closing PeerConnection")
	}
}
