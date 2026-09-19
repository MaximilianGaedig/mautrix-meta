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

// Call bridging, stage (b): 1:1 audio calls between Messenger and Matrix
// legacy VoIP (m.call.*), behind the call_bridging option.
//
// The bridge acts as one more device of the user (its whatsmeow E2EE
// device): it keeps the Messenger call-signalling channels open
// (callsignal), rings the portal with an m.call.invite from the caller's
// ghost, and when the user answers in Element it JOINs the Messenger call
// with a P2P answer whose DTLS fingerprint is signed with the device's
// Signal identity key (x-dtls-auth). Opus RTP is relayed between the two
// PeerConnections without transcoding (package callbridge). Outgoing calls
// (m.call.invite into a 1:1 portal) JOIN as caller with an offer, the flow
// captured from facebook.com.
//
// Only one call per login is handled at a time. Nothing secret is logged:
// no SDP, ICE credentials, candidate addresses, TURN credentials or keys.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"
	"github.com/rs/zerolog"
	"go.mau.fi/libsignal/ecc"
	waTypes "go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/connector/callbridge"
	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/callsignal"
	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

const (
	// callInviteLifetime is the m.call.invite lifetime and how long an
	// unanswered call rings on either side.
	callInviteLifetime = 60 * time.Second
	// callSetupTimeout bounds answering -> media connected.
	callSetupTimeout = 30 * time.Second
	// callGatherWait is how long the Matrix leg waits for ICE gathering
	// before sending its SDP (remaining candidates are trickled).
	callGatherWait = 3 * time.Second
	// callCandidateBatch batches trickled m.call.candidates.
	callCandidateBatch = 200 * time.Millisecond
	// callDisconnectGrace lets ICE recover from a transient disconnect.
	callDisconnectGrace = 10 * time.Second
)

// matrixCallEventTypes are the legacy VoIP events the bridge listens to.
var matrixCallEventTypes = []event.Type{
	event.CallInvite, event.CallCandidates, event.CallAnswer, event.CallReject,
	event.CallSelectAnswer, event.CallNegotiate, event.CallHangup,
}

// registerCallEventHandlers hooks m.call.* into the Matrix event processor;
// bridgev2 itself drops them.
func (m *MetaConnector) registerCallEventHandlers() {
	mx, ok := m.Bridge.Matrix.(*matrix.Connector)
	if !ok || mx.EventProcessor == nil {
		m.Bridge.Log.Warn().Msg("Matrix connector doesn't expose an event processor, call bridging can't receive m.call events")
		return
	}
	for _, t := range matrixCallEventTypes {
		mx.EventProcessor.On(t, m.handleMatrixCallEvent)
	}
}

func (m *MetaConnector) handleMatrixCallEvent(ctx context.Context, evt *event.Event) {
	if !m.Config.CallBridging || m.Bridge.IsGhostMXID(evt.Sender) || evt.Sender == m.Bridge.Bot.GetMXID() {
		return
	}
	log := m.Bridge.Log.With().
		Str("action", "handle matrix call event").
		Str("event_type", evt.Type.Type).
		Stringer("event_id", evt.ID).
		Stringer("room_id", evt.RoomID).
		Logger()
	ctx = log.WithContext(ctx)
	portal, err := m.Bridge.GetPortalByMXID(ctx, evt.RoomID)
	if err != nil || portal == nil {
		return
	}
	login, err := m.Bridge.GetExistingUserLoginByID(ctx, portal.Receiver)
	if err != nil || login == nil {
		user, err := m.Bridge.GetUserByMXID(ctx, evt.Sender)
		if err != nil || user == nil {
			return
		}
		login = user.GetDefaultLogin()
	}
	if login == nil || login.UserMXID != evt.Sender {
		log.Debug().Msg("Ignoring call event from a user without a login for this portal")
		return
	}
	client, ok := login.Client.(*MetaClient)
	if !ok {
		return
	}
	cb := client.callBridge.Load()
	if cb == nil {
		log.Debug().Msg("Ignoring call event: call bridging isn't running for this login")
		return
	}
	cb.handleMatrixEvent(ctx, portal, evt)
}

// callBridge is the per-login call bridging state.
type callBridge struct {
	m   *MetaClient
	sig *callsignal.Client
	log zerolog.Logger

	lock   sync.Mutex
	active *callSession
}

func (m *MetaClient) callBridgingEnabled() bool {
	return m.Main.Config.CallBridging
}

// startCallBridging opens the call-signalling channels. It is idempotent.
func (m *MetaClient) startCallBridging(ctx context.Context) {
	if !m.callBridgingEnabled() || m.Client == nil || m.callBridge.Load() != nil {
		return
	}
	log := m.UserLogin.Log.With().Str("component", "call bridge").Logger()
	if m.LoginMeta.CallDeviceID == "" {
		m.LoginMeta.CallDeviceID = uuid.NewString()
		if err := m.UserLogin.Save(ctx); err != nil {
			log.Warn().Err(err).Msg("Failed to save call device id")
		}
	}
	cb := &callBridge{m: m, log: log}
	sig, err := m.Client.NewCallSignalClient(m.LoginMeta.CallDeviceID, cb.handleSignal, log)
	if errors.Is(err, messagix.ErrCallsUnsupported) {
		log.Debug().Msg("Call bridging not supported on this platform")
		return
	} else if err != nil {
		log.Err(err).Msg("Failed to create call signalling client")
		return
	}
	cb.sig = sig
	if !m.callBridge.CompareAndSwap(nil, cb) {
		return
	}
	sig.Start(ctx)
	log.Info().Msg("Call bridging started")
}

// stopCallBridging ends any active call and closes the channels.
func (m *MetaClient) stopCallBridging() {
	cb := m.callBridge.Swap(nil)
	if cb == nil {
		return
	}
	cb.lock.Lock()
	s := cb.active
	cb.lock.Unlock()
	if s != nil {
		s.end(endBridgeShutdown, "")
	}
	cb.sig.Stop()
	cb.log.Info().Msg("Call bridging stopped")
}

// handleFBCallEnded lets the stage (a) fb:call "ended" notification end a
// session that is still ringing (e.g. the caller gave up).
func (m *MetaClient) handleFBCallEnded(callID string) {
	cb := m.callBridge.Load()
	if cb == nil || callID == "" {
		return
	}
	cb.lock.Lock()
	s := cb.active
	cb.lock.Unlock()
	if s != nil && s.serverInfo() == callID {
		s.log.Debug().Msg("fb:call notification says the call ended")
		s.end(endRemoteHangup, "")
	}
}

// --- session ---

type endReason int

const (
	endLocalHangup    endReason = iota // Matrix user hung up
	endLocalDecline                    // Matrix user declined a ring
	endRemoteHangup                    // Messenger peer hung up
	endDismissed                       // ringing ended on Messenger (answered/declined elsewhere, caller gave up)
	endTimeout                         // nobody answered
	endFailed                          // setup or media failure
	endBridgeShutdown                  // bridge disconnect
)

type callSession struct {
	cb       *callBridge
	m        *MetaClient
	ctx      context.Context
	cancel   context.CancelFunc
	log      zerolog.Logger
	incoming bool
	peerID   int64
	e2ee     bool
	// videoCodec is the codec both legs send video with, "" for audio calls.
	videoCodec string
	portal     *bridgev2.Portal
	ghost      bridgev2.MatrixAPI

	// Messenger side
	lock           sync.Mutex
	conference     string
	serverInfoData string
	ringOffer      string
	ringRelay      *rtcsignal.RelayInfo
	cc             *rtcsignal.CallContext
	metaLeg        *callbridge.Leg
	metaCands      []rtcsignal.IceCandidate // local candidates waiting for the JOIN
	remoteCands    []webrtc.ICECandidateInit
	joined         bool
	metaAnswered   bool
	metaPrepare    sync.Once
	metaAnswer     string // incoming: our signed answer, prepared while ringing
	metaAnswerErr  error
	mediaConnected atomic.Bool

	// Matrix side
	mxCallID      string
	mxPartyID     string
	mxRemoteParty string
	mxLeg         *callbridge.Leg
	mxAnswered    bool
	mxSnapshot    bool
	mxPreCands    []webrtc.ICECandidateInit
	mxCandBuf     []event.CallCandidate
	mxCandTimer   *time.Timer
	mxLocalSDP    string // outgoing: our answer, sent once Messenger answers

	endOnce sync.Once
}

func (s *callSession) serverInfo() string {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.serverInfoData
}

// matches reports whether a signalling message belongs to this call.
func (s *callSession) matches(msg *rtcsignal.Message) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	h := &msg.Header
	return (s.conference != "" && h.ConferenceName == s.conference) ||
		(s.serverInfoData != "" && h.ServerInfoData == s.serverInfoData)
}

// --- Messenger signalling ---

// handleSignal is the callsignal.Handler. It runs on the socket goroutine:
// it only picks the response and hands the work to goroutines.
func (cb *callBridge) handleSignal(ctx context.Context, msg *rtcsignal.Message, via callsignal.Transport) *rtcsignal.Message {
	cb.lock.Lock()
	s := cb.active
	cb.lock.Unlock()
	if ring := msg.Body.RingRequest; ring != nil {
		switch {
		case s != nil && s.matches(msg):
			// The server retries RING (every ~10 s) until answered.
			return nil
		case s != nil:
			callsignal.LogMessage(cb.log.Info(), msg).Msg("Already in a call, answering ring as busy")
			return rtcsignal.NewRingResponse(msg, rtcsignal.NewClientSessionID(), rtcsignal.DeviceStatusInAnotherCall)
		}
		callsignal.LogMessage(cb.log.Info(), msg).Str("via", string(via)).Msg("Incoming Messenger call")
		go cb.startIncoming(context.WithoutCancel(ctx), msg)
		return nil
	}
	if s == nil || !s.matches(msg) {
		return nil
	}
	return s.handleSignal(msg)
}

func (s *callSession) respond(msg *rtcsignal.Message, body rtcsignal.Body) *rtcsignal.Message {
	s.lock.Lock()
	cc := s.cc
	s.lock.Unlock()
	if cc == nil {
		return nil // not joined yet: the client's default reply
	}
	return cc.Respond(msg, body)
}

func (s *callSession) handleSignal(msg *rtcsignal.Message) *rtcsignal.Message {
	b := &msg.Body
	switch {
	case b.IceCandidateRequest != nil:
		for _, c := range b.IceCandidateRequest.Candidates {
			idx := uint16(c.SDPMLineIndex)
			mid := c.SDPMid
			s.addRemoteMetaCandidate(webrtc.ICECandidateInit{Candidate: c.Candidate, SDPMid: &mid, SDPMLineIndex: &idx})
		}
		s.log.Debug().Int("count", len(b.IceCandidateRequest.Candidates)).Msg("Received Messenger ICE candidates")
	case b.ServerMediaUpdateRequest != nil:
		return s.handleServerMediaUpdate(msg)
	case b.ConferenceStateRequest != nil:
		s.handleConferenceState(b.ConferenceStateRequest)
	case b.HangupRequest != nil:
		s.log.Info().Int32("reason", int32(b.HangupRequest.Reason)).Msg("Messenger peer hung up")
		go s.end(endRemoteHangup, "")
	case b.DismissRequest != nil:
		s.lock.Lock()
		joined := s.joined
		s.lock.Unlock()
		if joined {
			// Our own JOIN makes the server dismiss the ring on the user's
			// other devices; a DISMISS only matters while we ring.
			s.log.Debug().Int32("reason", int32(b.DismissRequest.Reason)).Msg("Ignoring DISMISS after joining")
			break
		}
		s.log.Info().Int32("reason", int32(b.DismissRequest.Reason)).Msg("Messenger ringing dismissed")
		go s.end(endDismissed, string(dismissHangupReason(b.DismissRequest.Reason)))
	case b.DataMessageRequest != nil && b.DataMessageRequest.Message != nil:
		// Group E2EE key messages only matter for SFU calls.
		s.log.Debug().Str("topic", b.DataMessageRequest.Message.Topic).Msg("Messenger call data message")
	}
	return s.respond(msg, rtcsignal.DefaultResponseBody(msg))
}

func dismissHangupReason(r rtcsignal.DismissReason) event.CallHangupReason {
	switch r {
	case rtcsignal.DismissAnsweredOnAnotherDevice:
		return "answered_elsewhere"
	case rtcsignal.DismissRejectedOnAnotherDevice, rtcsignal.DismissRejectedByCallee:
		return "user_busy"
	default:
		return event.CallHangupUserHangup
	}
}

func (s *callSession) handleConferenceState(cs *rtcsignal.ConferenceStateRequest) {
	peer := strconv.FormatInt(s.peerID, 10)
	for uid, st := range cs.ParticipantStates {
		who := "self"
		if uid == peer {
			who = "peer"
		}
		s.log.Debug().Str("who", who).Int32("state", int32(st.State)).Int64("version", cs.Version).Msg("Conference state")
	}
	st, ok := cs.ParticipantStates[peer]
	if !ok {
		return
	}
	switch st.State {
	case rtcsignal.StateDisconnected, rtcsignal.StateConnectionDropped:
		go s.end(endRemoteHangup, "")
	case rtcsignal.StateRejected, rtcsignal.StateNoAnswer, rtcsignal.StateUnreachable, rtcsignal.StateInAnotherCall:
		reason := event.CallHangupReason("user_busy")
		if st.State == rtcsignal.StateNoAnswer || st.State == rtcsignal.StateUnreachable {
			reason = event.CallHangupInviteTimeout
		}
		go s.end(endDismissed, string(reason))
	}
}

func (s *callSession) handleServerMediaUpdate(msg *rtcsignal.Message) *rtcsignal.Message {
	smu := msg.Body.ServerMediaUpdateRequest
	resp := &rtcsignal.ServerMediaUpdateResponse{CurrentVersion: smu.ToVersion}
	if smu.MediaPath == rtcsignal.MediaPathSFU {
		s.log.Warn().Msg("Messenger moved the call to its SFU, which the bridge can't join")
		go s.end(endFailed, "Messenger moved the call to a group-call server, which the bridge doesn't support")
		return s.respond(msg, rtcsignal.Body{ServerMediaUpdateResponse: resp})
	}
	sdpType, sd := smu.RemoteSDP()
	s.lock.Lock()
	leg := s.metaLeg
	s.lock.Unlock()
	if sd == nil || sd.SDP == "" || leg == nil {
		return s.respond(msg, rtcsignal.Body{ServerMediaUpdateResponse: resp})
	}
	origin, _ := strconv.ParseInt(smu.SDPOriginLocalID, 10, 64)
	if origin == 0 {
		origin = s.peerID
	}
	if err := s.verifyPeerSDP(sd.SDP, origin); err != nil {
		s.log.Warn().Err(err).Str("sdp_type", sdpType).Msg("Remote SDP failed x-dtls-auth verification")
		go s.end(endFailed, "Couldn't verify the Messenger peer's call encryption")
		return s.respond(msg, rtcsignal.Body{ServerMediaUpdateResponse: resp})
	}
	switch sdpType {
	case "answer":
		s.log.Info().Msg("Messenger peer answered")
		if err := leg.SetAnswer(callbridge.PrepareMetaRemoteSDP(sd.SDP)); err != nil {
			s.log.Err(err).Msg("Failed to apply Messenger answer")
			go s.end(endFailed, "Failed to apply the Messenger answer")
			break
		}
		s.lock.Lock()
		s.metaAnswered = true
		s.lock.Unlock()
		go s.answerMatrixOutgoing()
	case "offer":
		// A renegotiation: answer it in the SMU response.
		answer, err := leg.AnswerOffer(callbridge.PrepareMetaRemoteSDP(sd.SDP))
		if err == nil {
			answer, err = callbridge.PrepareMetaLocalSDP(answer, s.m.callIdentity(), s.videoCodec != "")
		}
		if err != nil {
			s.log.Err(err).Msg("Failed to answer Messenger renegotiation")
			break
		}
		s.log.Info().Msg("Answered Messenger renegotiation")
		resp.Answer = &rtcsignal.SessionDescription{SDP: answer}
	}
	return s.respond(msg, rtcsignal.Body{ServerMediaUpdateResponse: resp})
}

func (s *callSession) addRemoteMetaCandidate(c webrtc.ICECandidateInit) {
	s.lock.Lock()
	leg := s.metaLeg
	if leg == nil {
		s.remoteCands = append(s.remoteCands, c)
		s.lock.Unlock()
		return
	}
	s.lock.Unlock()
	if err := leg.AddCandidate(c); err != nil {
		s.log.Debug().Err(err).Msg("Failed to add Messenger ICE candidate")
	}
}

// onLocalMetaCandidate trickles our candidates to Messenger once joined.
func (s *callSession) onLocalMetaCandidate(c *webrtc.ICECandidateInit) {
	ic := rtcsignal.IceCandidate{Candidate: c.Candidate}
	if c.SDPMid != nil {
		ic.SDPMid = *c.SDPMid
	}
	if c.SDPMLineIndex != nil {
		ic.SDPMLineIndex = int64(*c.SDPMLineIndex)
	}
	s.lock.Lock()
	if !s.joined {
		s.metaCands = append(s.metaCands, ic)
		s.lock.Unlock()
		return
	}
	cc := s.cc
	s.lock.Unlock()
	s.sendMetaCandidates(cc, []rtcsignal.IceCandidate{ic})
}

func (s *callSession) sendMetaCandidates(cc *rtcsignal.CallContext, cands []rtcsignal.IceCandidate) {
	// The web client sends one candidate per message. Waiting for each
	// acknowledgement before the next cost ~40 ms per candidate, so the
	// queued ones go out together.
	for _, c := range cands {
		go func() {
			if _, err := s.cb.sig.Request(s.ctx, cc.NewIceCandidates(c)); err != nil && s.ctx.Err() == nil {
				s.log.Debug().Err(err).Msg("Failed to send ICE candidate to Messenger")
			}
		}()
	}
}

// --- identity ---

// callIdentity is the bridge device's Signal identity for x-dtls-auth.
func (m *MetaClient) callIdentity() *callbridge.Identity {
	dev := m.WADevice
	if dev == nil || dev.IdentityKey == nil || dev.ID == nil {
		return nil
	}
	return &callbridge.Identity{
		UserID:   m.selfFBID(),
		DeviceID: int32(dev.ID.Device),
		Priv:     *dev.IdentityKey.Priv,
		Pub:      *dev.IdentityKey.Pub,
	}
}

// callE2eeState is our E2eeClientState for the JOIN, shaped like the web
// client's (cipher suite 2, versions 4-7, persistent identity key). It is
// only consumed by SFU (group) E2EE; for P2P it just has to be well-formed.
// The one-time pre-key slot repeats the signed pre-key: the bridge doesn't
// take part in SFU E2EE.
func (m *MetaClient) callE2eeState() []byte {
	dev := m.WADevice
	if dev == nil || dev.IdentityKey == nil || dev.SignedPreKey == nil || dev.ID == nil {
		return nil
	}
	spk := append([]byte{ecc.DjbType}, dev.SignedPreKey.Pub[:]...)
	var sig []byte
	if dev.SignedPreKey.Signature != nil {
		sig = dev.SignedPreKey.Signature[:]
	}
	bundle := &rtcsignal.PreKeyBundle{
		IdentityKey:     append([]byte{ecc.DjbType}, dev.IdentityKey.Pub[:]...),
		SignedPreKeyID:  int32(dev.SignedPreKey.KeyID),
		SignedPreKey:    spk,
		SignedPreKeySig: sig,
		PreKeyID:        int32(dev.SignedPreKey.KeyID),
		PreKey:          spk,
	}
	return (&rtcsignal.E2eeClientState{
		PreKeyBundle:       bundle.Marshal(),
		CipherSuites:       []int16{2},
		SupportedVersions:  []rtcsignal.VersionRange{{Min: 4, Max: 7}},
		AllowOptionalGfd:   true,
		IdentityKeyMode:    rtcsignal.IdentityKeyModePersistent,
		DeviceID:           int32(dev.ID.Device),
		KeyNegotiationProt: []int32{0},
	}).Marshal()
}

// verifyPeerSDP checks the x-dtls-auth of a Messenger SDP from userID: the
// signature must be valid, and the signing identity key must not contradict
// the key whatsmeow holds for that device. An unknown device is accepted
// with a warning (whatsmeow may simply never have fetched it).
func (s *callSession) verifyPeerSDP(sdp string, userID int64) error {
	info, err := rtcsignal.VerifyDTLSAuth(sdp, userID, nil)
	if err != nil {
		return err
	}
	dev := s.m.WADevice
	if dev == nil || dev.Identities == nil || len(info.PublicKey) != 33 {
		s.log.Warn().Msg("Can't compare the peer's x-dtls-auth key with the identity store")
		return nil
	}
	addr := waTypes.JID{User: strconv.FormatInt(userID, 10), Device: uint16(info.DeviceID), Server: waTypes.MessengerServer}.
		SignalAddress().String()
	key := [32]byte(info.PublicKey[1:])
	trusted, err := dev.Identities.IsTrustedIdentity(s.ctx, addr, key)
	if err != nil {
		return fmt.Errorf("identity store: %w", err)
	} else if !trusted {
		return rtcsignal.ErrDtlsAuthKeyMismatch
	}
	// IsTrustedIdentity also accepts unknown addresses; probe with a key
	// that can't be stored to tell "matches" from "unknown".
	var zero [32]byte
	if unknown, _ := dev.Identities.IsTrustedIdentity(s.ctx, addr, zero); unknown {
		s.log.Warn().Int32("peer_device", info.DeviceID).Msg("Peer's x-dtls-auth signature is valid, but its device isn't in the identity store")
	} else {
		s.log.Info().Int32("peer_device", info.DeviceID).Msg("Peer's x-dtls-auth key matches the identity store")
	}
	return nil
}

// --- ICE servers ---

func (s *callSession) metaICEServers() []webrtc.ICEServer {
	servers := []webrtc.ICEServer{{URLs: []string{messagix.MessengerSTUNServer}}}
	turn, err := s.m.Client.FetchTURNServer(s.ctx)
	if err != nil {
		s.log.Warn().Err(err).Msg("Failed to fetch Messenger TURN server")
	} else if urls := turn.URLs(); len(urls) > 0 {
		servers = append(servers, webrtc.ICEServer{URLs: urls, Username: turn.Username, Credential: turn.Password})
		s.log.Debug().Int("url_count", len(urls)).Msg("Got Messenger TURN server")
	}
	s.lock.Lock()
	ri := s.ringRelay
	s.lock.Unlock()
	if ri != nil {
		for _, t := range ri.Turns {
			var urls []string
			if t.IPv4 != "" && t.UDPPort != 0 {
				urls = append(urls, fmt.Sprintf("turn:%s:%d?transport=udp", t.IPv4, t.UDPPort))
			}
			if t.IPv4 != "" && t.SSLTCPPort != 0 {
				urls = append(urls, fmt.Sprintf("turn:%s:%d?transport=tcp", t.IPv4, t.SSLTCPPort))
			}
			user, pass := t.TurnUsername, t.TurnPassword
			if user == "" {
				user, pass = ri.TurnUsername, ri.TurnPassword
			}
			if len(urls) > 0 && user != "" {
				servers = append(servers, webrtc.ICEServer{URLs: urls, Username: user, Credential: pass})
			}
		}
	}
	return servers
}

func (s *callSession) matrixICEServers() []webrtc.ICEServer {
	asIntent, ok := s.ghost.(*matrix.ASIntent)
	if !ok {
		return nil
	}
	resp, err := asIntent.Matrix.TurnServer(s.ctx)
	if err != nil {
		s.log.Warn().Err(err).Msg("Failed to fetch homeserver TURN server, using no relay on the Matrix leg")
		return nil
	} else if len(resp.URIs) == 0 {
		return nil
	}
	s.log.Debug().Int("url_count", len(resp.URIs)).Msg("Got homeserver TURN server")
	return []webrtc.ICEServer{{URLs: resp.URIs, Username: resp.Username, Credential: resp.Password}}
}

// --- session lifecycle ---

// newSession registers a new active call. mxCallID is Element's call id
// for outgoing calls; incoming calls get a fresh one.
func (cb *callBridge) newSession(ctx context.Context, portal *bridgev2.Portal, peerID int64, incoming bool, mxCallID, conference string) (*callSession, error) {
	if mxCallID == "" {
		mxCallID = uuid.NewString()
	}
	ghost, err := cb.m.Main.Bridge.GetGhostByID(ctx, metaid.MakeUserID(peerID))
	if err != nil || ghost == nil {
		return nil, fmt.Errorf("failed to get ghost: %w", err)
	}
	sctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s := &callSession{
		cb:        cb,
		m:         cb.m,
		ctx:       sctx,
		cancel:    cancel,
		incoming:  incoming,
		peerID:    peerID,
		portal:    portal,
		ghost:     ghost.Intent,
		e2ee:      true,
		mxCallID:  mxCallID,
		mxPartyID: "bridge-" + uuid.NewString()[:8],
	}
	s.log = cb.log.With().
		Str("call_dir", map[bool]string{true: "incoming", false: "outgoing"}[incoming]).
		Str("portal_id", string(portal.ID)).
		Str("mx_call_id", mxCallID).
		Str("conference", conference).
		Logger()
	cb.lock.Lock()
	defer cb.lock.Unlock()
	if cb.active != nil {
		cancel()
		return nil, errBusy
	}
	cb.active = s
	return s, nil
}

var errBusy = errors.New("already in a call")

func (cb *callBridge) findDMPortal(ctx context.Context, peerID int64) (*bridgev2.Portal, error) {
	key := networkid.PortalKey{ID: networkid.PortalID(strconv.FormatInt(peerID, 10)), Receiver: cb.m.UserLogin.ID}
	portal, err := cb.m.Main.Bridge.GetExistingPortalByKey(ctx, key)
	if err != nil {
		return nil, err
	} else if portal == nil || portal.MXID == "" {
		return nil, fmt.Errorf("no Matrix room for the chat with the caller")
	}
	return portal, nil
}

func (cb *callBridge) startIncoming(ctx context.Context, msg *rtcsignal.Message) {
	ring := msg.Body.RingRequest
	log := cb.log.With().Str("conference", msg.Header.ConferenceName).Logger()
	caller, err := strconv.ParseInt(ring.Caller, 10, 64)
	if err != nil || caller == 0 {
		log.Warn().Msg("Ring without a caller id, ignoring")
		return
	}
	if caller == cb.m.selfFBID() {
		log.Debug().Msg("Ring for a call placed by the user, ignoring")
		return
	}
	if ring.Offer == nil || ring.Offer.SDP == "" {
		log.Info().Int32("ring_type", int32(ring.RingType)).Int32("media_path", int32(ring.MediaPath)).
			Msg("Ring without a P2P offer (group call?), not bridging; stage (a) notices still apply")
		return
	}
	if len(ring.OtherParticipants) > 1 {
		log.Info().Int("participants", len(ring.OtherParticipants)).Msg("Group call ring, not bridging")
		return
	}
	portal, err := cb.findDMPortal(ctx, caller)
	if err != nil {
		log.Warn().Err(err).Msg("Can't bridge incoming call")
		return
	}
	s, err := cb.newSession(ctx, portal, caller, true, "", msg.Header.ConferenceName)
	if err != nil {
		log.Info().Err(err).Msg("Not bridging incoming call")
		return
	}
	s.lock.Lock()
	s.conference = msg.Header.ConferenceName
	s.serverInfoData = msg.Header.ServerInfoData
	s.ringOffer = ring.Offer.SDP
	s.ringRelay = ring.RelayInfo
	if ring.RingType == rtcsignal.RingPeerVideo || callbridge.SendsVideo(ring.Offer.SDP) {
		s.videoCodec = callbridge.PickVideoCodec(ring.Offer.SDP)
	}
	s.e2ee = ring.E2eeEnforcement == nil || ring.E2eeEnforcement.Mode == rtcsignal.E2eeMandated
	s.lock.Unlock()
	s.log.Info().
		Int32("media_path", int32(ring.MediaPath)).
		Int32("ring_type", int32(ring.RingType)).
		Str("video_codec", s.videoCodec).
		Bool("e2ee", s.e2ee).
		Bool("relay_info", ring.RelayInfo != nil).
		Int16("retry", msg.Header.RetryCount).
		Msg("Ringing Matrix for incoming Messenger call")
	if err = s.verifyPeerSDP(ring.Offer.SDP, caller); err != nil {
		s.log.Warn().Err(err).Msg("Caller's x-dtls-auth doesn't verify, not bridging")
		s.end(endFailed, "")
		return
	}
	if err = s.ringMatrix(); err != nil {
		s.log.Err(err).Msg("Failed to ring Matrix")
		s.end(endFailed, "")
		return
	}
	go func() {
		if _, err := s.prepareMessengerAnswer(); err != nil {
			s.log.Warn().Err(err).Msg("Failed to prepare Messenger answer while ringing")
		}
	}()
	time.AfterFunc(callInviteLifetime, func() {
		s.lock.Lock()
		answered := s.mxAnswered
		s.lock.Unlock()
		if !answered && s.ctx.Err() == nil {
			s.log.Info().Msg("Nobody answered in Matrix")
			s.end(endTimeout, "")
		}
	})
}

// ringMatrix creates the Matrix leg and sends m.call.invite from the
// caller's ghost.
func (s *callSession) ringMatrix() error {
	leg, err := callbridge.NewLeg(callbridge.LegConfig{
		Name: "matrix", ICEServers: s.matrixICEServers(), VideoCodec: s.videoCodec, Log: s.log,
	})
	if err != nil {
		return err
	}
	s.lock.Lock()
	s.mxLeg = leg
	s.lock.Unlock()
	leg.OnCandidate(s.onLocalMatrixCandidate)
	leg.OnState(s.onLegState)
	if _, err = leg.CreateOffer(); err != nil {
		return err
	}
	sdp := s.matrixSnapshot(leg)
	_, err = s.ghost.SendMessage(s.ctx, s.portal.MXID, event.CallInvite, &event.Content{Parsed: &event.CallInviteEventContent{
		BaseCallEventContent: s.baseCallContent(),
		Lifetime:             int(callInviteLifetime / time.Millisecond),
		Offer:                event.CallData{SDP: sdp, Type: event.CallDataTypeOffer},
	}}, nil)
	if err == nil {
		s.log.Info().Msg("Sent m.call.invite")
	}
	return err
}

func (s *callSession) baseCallContent() event.BaseCallEventContent {
	return event.BaseCallEventContent{CallID: s.mxCallID, PartyID: s.mxPartyID, Version: "1"}
}

// matrixSnapshot waits for ICE gathering, takes the local SDP and switches
// the Matrix leg to trickling. Candidates gathered but missing from the SDP
// are trickled right away.
func (s *callSession) matrixSnapshot(leg *callbridge.Leg) string {
	sdp := leg.WaitGathering(s.ctx, callGatherWait)
	s.lock.Lock()
	s.mxSnapshot = true
	pre := s.mxPreCands
	s.mxPreCands = nil
	s.lock.Unlock()
	for _, c := range pre {
		if !strings.Contains(sdp, strings.TrimPrefix(c.Candidate, "candidate:")) {
			s.queueMatrixCandidate(c)
		}
	}
	return sdp
}

func (s *callSession) onLocalMatrixCandidate(c *webrtc.ICECandidateInit) {
	s.lock.Lock()
	if !s.mxSnapshot {
		s.mxPreCands = append(s.mxPreCands, *c)
		s.lock.Unlock()
		return
	}
	s.lock.Unlock()
	s.queueMatrixCandidate(*c)
}

func (s *callSession) queueMatrixCandidate(c webrtc.ICECandidateInit) {
	cand := event.CallCandidate{Candidate: c.Candidate}
	if c.SDPMid != nil {
		cand.SDPMID = *c.SDPMid
	}
	if c.SDPMLineIndex != nil {
		cand.SDPMLineIndex = int(*c.SDPMLineIndex)
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	s.mxCandBuf = append(s.mxCandBuf, cand)
	if s.mxCandTimer == nil {
		s.mxCandTimer = time.AfterFunc(callCandidateBatch, s.flushMatrixCandidates)
	}
}

func (s *callSession) flushMatrixCandidates() {
	s.lock.Lock()
	cands := s.mxCandBuf
	s.mxCandBuf = nil
	s.mxCandTimer = nil
	s.lock.Unlock()
	if len(cands) == 0 || s.ctx.Err() != nil {
		return
	}
	_, err := s.ghost.SendMessage(s.ctx, s.portal.MXID, event.CallCandidates, &event.Content{Parsed: &event.CallCandidatesEventContent{
		BaseCallEventContent: s.baseCallContent(),
		Candidates:           cands,
	}}, nil)
	if err != nil {
		s.log.Warn().Err(err).Msg("Failed to send m.call.candidates")
	}
}

// onLegState ends the call when either PeerConnection fails for good and
// reports MEDIA_CONNECTED once the Messenger leg is up.
func (s *callSession) onLegState(state webrtc.PeerConnectionState) {
	switch state {
	case webrtc.PeerConnectionStateFailed:
		go s.end(endFailed, "")
	case webrtc.PeerConnectionStateDisconnected:
		time.AfterFunc(callDisconnectGrace, func() {
			s.lock.Lock()
			legs := []*callbridge.Leg{s.metaLeg, s.mxLeg}
			s.lock.Unlock()
			for _, l := range legs {
				if l != nil && l.PC.ConnectionState() == webrtc.PeerConnectionStateDisconnected {
					s.log.Warn().Str("leg", l.Name).Msg("PeerConnection stayed disconnected")
					s.end(endFailed, "")
					return
				}
			}
		})
	}
}

func (s *callSession) onMetaState(state webrtc.PeerConnectionState) {
	s.onLegState(state)
	if state != webrtc.PeerConnectionStateConnected || !s.mediaConnected.CompareAndSwap(false, true) {
		return
	}
	s.log.Info().Msg("Messenger media connected")
	s.lock.Lock()
	cc := s.cc
	s.lock.Unlock()
	if cc != nil {
		go func() {
			if _, err := s.cb.sig.Request(s.ctx, cc.NewMediaConnected()); err != nil && s.ctx.Err() == nil {
				s.log.Debug().Err(err).Msg("CLIENT_EVENT MEDIA_CONNECTED failed")
			}
		}()
	}
}

// startRelay forwards audio in both directions once both remote tracks
// exist.
func (s *callSession) startRelay() {
	s.lock.Lock()
	metaLeg, mxLeg := s.metaLeg, s.mxLeg
	s.lock.Unlock()
	relay := func(from, to *callbridge.Leg) {
		tr, err := from.RemoteTrack(s.ctx)
		if err != nil {
			if s.ctx.Err() == nil {
				s.log.Warn().Err(err).Str("from", from.Name).Msg("No remote audio")
			}
			return
		}
		var stats callbridge.RelayStats
		rlog := s.log.With().Str("from", from.Name).Str("to", to.Name).Logger()
		err = callbridge.Relay(s.ctx, tr, uint8(tr.PayloadType()), to.Local, &stats, rlog)
		rlog.Info().AnErr("relay_err", err).
			Uint64("forwarded", stats.Forwarded.Load()).
			Uint64("dropped", stats.Dropped.Load()).
			Msg("Audio relay stopped")
	}
	go relay(metaLeg, mxLeg)
	go relay(mxLeg, metaLeg)
	if metaLeg.LocalVideo != nil && mxLeg.LocalVideo != nil {
		go s.relayVideo(metaLeg, mxLeg)
		go s.relayVideo(mxLeg, metaLeg)
	}
	time.AfterFunc(callSetupTimeout, func() {
		if !s.mediaConnected.Load() && s.ctx.Err() == nil {
			s.log.Warn().Msg("Messenger media didn't connect in time")
			s.end(endFailed, "The call didn't connect")
		}
	})
}

// relayVideo forwards one direction of video and passes the receiver's
// keyframe requests back to the sender. A keyframe is also requested a few
// times at the start, so the picture appears without waiting for the
// sender's next periodic keyframe.
func (s *callSession) relayVideo(from, to *callbridge.Leg) {
	tr, err := from.RemoteVideoTrack(s.ctx)
	if err != nil {
		if s.ctx.Err() == nil {
			s.log.Debug().Err(err).Str("from", from.Name).Msg("No remote video")
		}
		return
	}
	ssrc := tr.SSRC()
	var lastPLI atomic.Int64
	requestKeyframe := func() {
		// At most one PLI per 500 ms towards the sender.
		now := time.Now().UnixMilli()
		if last := lastPLI.Load(); now-last < 500 || !lastPLI.CompareAndSwap(last, now) {
			return
		}
		from.RequestKeyframe(ssrc)
	}
	to.OnKeyframeRequest(requestKeyframe)
	for _, d := range []time.Duration{0, time.Second, 3 * time.Second} {
		time.AfterFunc(d, func() {
			if s.ctx.Err() == nil {
				requestKeyframe()
			}
		})
	}
	var stats callbridge.RelayStats
	rlog := s.log.With().Str("from", from.Name).Str("to", to.Name).Str("codec", tr.Codec().MimeType).Logger()
	err = callbridge.RelayVideo(s.ctx, tr, uint8(tr.PayloadType()), to.LocalVideo, &stats, rlog)
	rlog.Info().AnErr("relay_err", err).
		Uint64("forwarded", stats.Forwarded.Load()).
		Uint64("dropped", stats.Dropped.Load()).
		Msg("Video relay stopped")
}

// newMetaLeg creates the Messenger PeerConnection with Messenger's TURN.
func (s *callSession) newMetaLeg() (*callbridge.Leg, error) {
	leg, err := callbridge.NewLeg(callbridge.LegConfig{
		Name: "messenger", ICEServers: s.metaICEServers(), WebShape: true, VideoCodec: s.videoCodec, Log: s.log,
	})
	if err != nil {
		return nil, err
	}
	leg.OnCandidate(s.onLocalMetaCandidate)
	leg.OnState(s.onMetaState)
	s.lock.Lock()
	s.metaLeg = leg
	pending := s.remoteCands
	s.remoteCands = nil
	s.lock.Unlock()
	for _, c := range pending {
		_ = leg.AddCandidate(c)
	}
	return leg, nil
}

// join sends the JOIN and processes the response.
func (s *callSession) join(cc *rtcsignal.CallContext, params *rtcsignal.JoinParams) error {
	resp, err := s.cb.sig.RequestHook(s.ctx, cc.NewJoin(params), func(resp *rtcsignal.Message) {
		if resp.Header.ResponseStatusCode != rtcsignal.StatusOK {
			return
		}
		// Adopt the conference before the server's first pushes for it
		// (CONFERENCE_STATE, ICE candidates) are dispatched.
		s.lock.Lock()
		if cc.ConferenceName == "" {
			cc.ConferenceName = resp.Header.ConferenceName
			cc.ServerInfoData = resp.Header.ServerInfoData
			s.conference = resp.Header.ConferenceName
			s.serverInfoData = resp.Header.ServerInfoData
		}
		s.cc = cc
		s.lock.Unlock()
	})
	if err != nil {
		return fmt.Errorf("JOIN failed: %w", err)
	}
	jr := resp.Body.JoinResponse
	if jr == nil {
		return fmt.Errorf("JOIN response without body")
	}
	s.log.Info().
		Int32("media_path", int32(jr.MediaPath)).
		Bool("has_answer", jr.Answer != nil && jr.Answer.SDP != "").
		Bool("relay_info", jr.RelayInfo != nil).
		Msg("Joined Messenger call")
	if jr.MediaPath == rtcsignal.MediaPathSFU {
		return errSFUPath
	}
	s.lock.Lock()
	s.joined = true
	cands := s.metaCands
	s.metaCands = nil
	s.lock.Unlock()
	go s.sendMetaCandidates(cc, cands)
	go func() {
		if _, err := s.cb.sig.Request(s.ctx, cc.NewDominantSpeakerSubscription()); err != nil && s.ctx.Err() == nil {
			s.log.Debug().Err(err).Msg("SUBSCRIPTION failed")
		}
	}()
	return nil
}

// prepareMessengerAnswer creates the Messenger leg and its signed answer to
// the ring's offer while Matrix is still ringing, so ICE gathering (and the
// TURN allocations) are done by the time the user answers.
func (s *callSession) prepareMessengerAnswer() (string, error) {
	// A second caller (the user answering mid-preparation) waits for the first.
	s.metaPrepare.Do(func() {
		s.metaAnswer, s.metaAnswerErr = s.buildMessengerAnswer()
	})
	return s.metaAnswer, s.metaAnswerErr
}

func (s *callSession) buildMessengerAnswer() (string, error) {
	id := s.m.callIdentity()
	if id == nil {
		return "", errNoCallIdentity
	}
	leg, err := s.newMetaLeg()
	if err != nil {
		return "", fmt.Errorf("create Messenger PeerConnection: %w", err)
	}
	if s.ctx.Err() != nil {
		// The call ended while ringing, after end() collected the legs.
		leg.Close()
		return "", s.ctx.Err()
	}
	s.lock.Lock()
	offer := s.ringOffer
	s.lock.Unlock()
	answer, err := leg.AnswerOffer(callbridge.PrepareMetaRemoteSDP(offer))
	if err == nil {
		answer, err = callbridge.PrepareMetaLocalSDP(answer, id, s.videoCodec != "")
	}
	if err != nil {
		return "", fmt.Errorf("create Messenger answer: %w", err)
	}
	return answer, nil
}

var errNoCallIdentity = errors.New("no E2EE device identity to sign x-dtls-auth")

// errSFUPath: the server put the call on its SFU (group call path), which
// the bridge can't join.
var errSFUPath = errors.New("server chose the SFU media path")

// answerMessenger runs when the Matrix user answered: JOIN the Messenger
// call with an answer to the ring's offer.
func (s *callSession) answerMessenger() {
	answer, err := s.prepareMessengerAnswer()
	if errors.Is(err, errNoCallIdentity) {
		s.log.Error().Msg("No E2EE device identity, can't sign x-dtls-auth")
		s.end(endFailed, "The bridge has no Messenger encryption device, so it can't answer calls")
		return
	} else if err != nil {
		s.log.Err(err).Msg("Failed to prepare Messenger answer")
		s.end(endFailed, "")
		return
	}
	s.lock.Lock()
	leg := s.metaLeg
	cc := rtcsignal.NewCallContext(strconv.FormatInt(s.m.selfFBID(), 10), s.conference, s.serverInfoData)
	s.lock.Unlock()
	s.startRelay()
	if err = s.join(cc, &rtcsignal.JoinParams{
		Answer:       answer,
		PeerID:       strconv.FormatInt(s.peerID, 10),
		AudioTrackID: leg.TrackID,
		VideoTrackID: leg.VideoTrackID,
		E2eeState:    s.m.callE2eeState(),
		E2eeMandated: s.e2ee,
	}); err != nil {
		s.log.Err(err).Msg("Failed to join Messenger call")
		s.end(endFailed, "Failed to join the Messenger call")
		return
	}
}

// answerMatrixOutgoing sends m.call.answer once the Messenger peer answered
// an outgoing call.
func (s *callSession) answerMatrixOutgoing() {
	s.lock.Lock()
	sdp := s.mxLocalSDP
	s.mxAnswered = true
	s.lock.Unlock()
	if sdp == "" {
		return
	}
	_, err := s.ghost.SendMessage(s.ctx, s.portal.MXID, event.CallAnswer, &event.Content{Parsed: &event.CallAnswerEventContent{
		BaseCallEventContent: s.baseCallContent(),
		Answer:               event.CallData{SDP: sdp, Type: event.CallDataTypeAnswer},
	}}, nil)
	if err != nil {
		s.log.Err(err).Msg("Failed to send m.call.answer")
		s.end(endFailed, "")
		return
	}
	s.log.Info().Msg("Sent m.call.answer")
	s.startRelay()
}

// startOutgoing handles m.call.invite from the Matrix user in a 1:1 portal.
func (cb *callBridge) startOutgoing(ctx context.Context, portal *bridgev2.Portal, inv *event.CallInviteEventContent) {
	log := zerolog.Ctx(ctx)
	meta, _ := portal.Metadata.(*metaid.PortalMetadata)
	peerID := metaid.ParseFBPortalID(portal.ID)
	if peerID == 0 || meta == nil || !(meta.ThreadType.IsOneToOne() || meta.WhatsAppServer == waTypes.MessengerServer) {
		log.Info().Msg("Call invite in a non-1:1 portal, not bridging")
		return
	}
	if peerID == cb.m.selfFBID() {
		return
	}
	s, err := cb.newSession(ctx, portal, peerID, false, inv.CallID, "")
	if errors.Is(err, errBusy) {
		cb.rejectBusy(ctx, portal, peerID, inv)
		return
	} else if err != nil {
		log.Err(err).Msg("Failed to start outgoing call")
		return
	}
	s.lock.Lock()
	s.e2ee = meta.WhatsAppServer != ""
	s.mxRemoteParty = inv.PartyID
	if callbridge.SendsVideo(inv.Offer.SDP) {
		s.videoCodec = callbridge.PickVideoCodec(inv.Offer.SDP)
	}
	s.lock.Unlock()
	s.log.Info().Bool("e2ee", s.e2ee).Str("video_codec", s.videoCodec).Msg("Outgoing call from Matrix")
	id := s.m.callIdentity()
	if id == nil {
		s.end(endFailed, "The bridge has no Messenger encryption device, so it can't place calls")
		return
	}
	// Matrix leg: answer Element's offer now, send the answer when the
	// Messenger peer picks up.
	mxLeg, err := callbridge.NewLeg(callbridge.LegConfig{
		Name: "matrix", ICEServers: s.matrixICEServers(), VideoCodec: s.videoCodec, Log: s.log,
	})
	if err != nil {
		s.log.Err(err).Msg("Failed to create Matrix PeerConnection")
		s.end(endFailed, "")
		return
	}
	s.lock.Lock()
	s.mxLeg = mxLeg
	s.lock.Unlock()
	mxLeg.OnCandidate(s.onLocalMatrixCandidate)
	mxLeg.OnState(s.onLegState)
	if _, err = mxLeg.AnswerOffer(inv.Offer.SDP); err != nil {
		s.log.Err(err).Msg("Failed to answer Element's offer")
		s.end(endFailed, "")
		return
	}
	sdp := s.matrixSnapshot(mxLeg)
	s.lock.Lock()
	s.mxLocalSDP = sdp
	s.lock.Unlock()

	err = s.placeMessengerCall(id, peerID)
	if errors.Is(err, errSFUPath) {
		// Right after an earlier call in the thread, Messenger tends to put
		// the next one on its SFU; placing it again gets the P2P path.
		s.log.Info().Msg("Messenger chose the SFU path, placing the call again")
		s.abandonMessengerAttempt()
		err = s.placeMessengerCall(id, peerID)
	}
	if err != nil {
		s.log.Err(err).Msg("Failed to start Messenger call")
		s.end(endFailed, "Failed to start the Messenger call")
		return
	}
	lifetime := callInviteLifetime
	if inv.Lifetime > 0 {
		lifetime = min(time.Duration(inv.Lifetime)*time.Millisecond, 2*callInviteLifetime)
	}
	time.AfterFunc(lifetime, func() {
		s.lock.Lock()
		answered := s.metaAnswered
		s.lock.Unlock()
		if !answered && s.ctx.Err() == nil {
			s.log.Info().Msg("Messenger peer didn't answer")
			s.end(endTimeout, "")
		}
	})
}

// placeMessengerCall creates the Messenger leg and JOINs with an offer that
// rings the peer.
func (s *callSession) placeMessengerCall(id *callbridge.Identity, peerID int64) error {
	metaLeg, err := s.newMetaLeg()
	if err != nil {
		return fmt.Errorf("create Messenger PeerConnection: %w", err)
	}
	offer, err := metaLeg.CreateOffer()
	if err == nil {
		offer, err = callbridge.PrepareMetaLocalSDP(offer, id, s.videoCodec != "")
	}
	if err != nil {
		return fmt.Errorf("create Messenger offer: %w", err)
	}
	peer := strconv.FormatInt(peerID, 10)
	cc := rtcsignal.NewCallContext(strconv.FormatInt(s.m.selfFBID(), 10), "", "")
	return s.join(cc, &rtcsignal.JoinParams{
		Offer:        offer,
		PeerID:       peer,
		UsersToCall:  []string{peer},
		AudioTrackID: metaLeg.TrackID,
		VideoTrackID: metaLeg.VideoTrackID,
		E2eeState:    s.m.callE2eeState(),
		E2eeMandated: s.e2ee,
	})
}

// abandonMessengerAttempt leaves the conference of a failed JOIN and drops
// its leg, so the call can be placed again.
func (s *callSession) abandonMessengerAttempt() {
	s.lock.Lock()
	cc, leg := s.cc, s.metaLeg
	s.cc, s.metaLeg, s.joined = nil, nil, false
	s.conference, s.serverInfoData = "", ""
	s.metaCands, s.remoteCands = nil, nil
	s.lock.Unlock()
	if cc != nil {
		ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
		if _, err := s.cb.sig.Request(ctx, cc.NewHangup(rtcsignal.HangupHangupCall)); err != nil && s.ctx.Err() == nil {
			s.log.Debug().Err(err).Msg("Failed to leave the SFU conference")
		}
		cancel()
	}
	leg.Close()
}

func (cb *callBridge) rejectBusy(ctx context.Context, portal *bridgev2.Portal, peerID int64, inv *event.CallInviteEventContent) {
	ghost, err := cb.m.Main.Bridge.GetGhostByID(ctx, metaid.MakeUserID(peerID))
	if err != nil || ghost == nil {
		return
	}
	_, _ = ghost.Intent.SendMessage(ctx, portal.MXID, event.CallHangup, &event.Content{Parsed: &event.CallHangupEventContent{
		BaseCallEventContent: event.BaseCallEventContent{CallID: inv.CallID, PartyID: "bridge-busy", Version: "1"},
		Reason:               "user_busy",
	}}, nil)
}

// --- Matrix events ---

func parseCallContent[T any](evt *event.Event) (*T, bool) {
	if parsed, ok := evt.Content.Parsed.(*T); ok {
		return parsed, true
	}
	var out T
	if err := json.Unmarshal(evt.Content.VeryRaw, &out); err != nil {
		return nil, false
	}
	return &out, true
}

func (cb *callBridge) handleMatrixEvent(ctx context.Context, portal *bridgev2.Portal, evt *event.Event) {
	log := zerolog.Ctx(ctx)
	if evt.Type == event.CallInvite {
		inv, ok := parseCallContent[event.CallInviteEventContent](evt)
		if !ok {
			return
		}
		go cb.startOutgoing(context.WithoutCancel(ctx), portal, inv)
		return
	}
	var base *event.BaseCallEventContent
	switch evt.Type {
	case event.CallAnswer:
		if c, ok := parseCallContent[event.CallAnswerEventContent](evt); ok {
			base = &c.BaseCallEventContent
		}
	default:
		if c, ok := parseCallContent[event.BaseCallEventContent](evt); ok {
			base = c
		}
	}
	if base == nil {
		return
	}
	cb.lock.Lock()
	s := cb.active
	cb.lock.Unlock()
	if s == nil || s.mxCallID != base.CallID || s.portal.MXID != portal.MXID {
		log.Debug().Msg("Call event for an unknown call")
		return
	}
	if base.PartyID == s.mxPartyID {
		return // our own echo
	}
	switch evt.Type {
	case event.CallAnswer:
		ans, _ := parseCallContent[event.CallAnswerEventContent](evt)
		go s.onMatrixAnswer(ans)
	case event.CallCandidates:
		cands, ok := parseCallContent[event.CallCandidatesEventContent](evt)
		if !ok {
			return
		}
		s.lock.Lock()
		leg := s.mxLeg
		s.lock.Unlock()
		for _, c := range cands.Candidates {
			mid, idx := c.SDPMID, uint16(c.SDPMLineIndex)
			if leg != nil {
				_ = leg.AddCandidate(webrtc.ICECandidateInit{Candidate: c.Candidate, SDPMid: &mid, SDPMLineIndex: &idx})
			}
		}
		s.log.Debug().Int("count", len(cands.Candidates)).Msg("Received Matrix ICE candidates")
	case event.CallHangup, event.CallReject:
		s.log.Info().Str("event_type", evt.Type.Type).Msg("Matrix user ended the call")
		s.lock.Lock()
		answered := s.mxAnswered
		s.lock.Unlock()
		if s.incoming && !answered {
			go s.end(endLocalDecline, "")
		} else {
			go s.end(endLocalHangup, "")
		}
	case event.CallSelectAnswer:
		// Outgoing: Element selects our answer. Nothing to do.
	case event.CallNegotiate:
		s.log.Warn().Msg("m.call.negotiate isn't supported, ignoring")
	}
}

func (s *callSession) onMatrixAnswer(ans *event.CallAnswerEventContent) {
	if ans == nil || !s.incoming {
		return
	}
	s.lock.Lock()
	if s.mxAnswered {
		s.lock.Unlock()
		return
	}
	s.mxAnswered = true
	s.mxRemoteParty = ans.PartyID
	leg := s.mxLeg
	s.lock.Unlock()
	s.log.Info().Msg("Matrix user answered")
	if err := leg.SetAnswer(ans.Answer.SDP); err != nil {
		s.log.Err(err).Msg("Failed to apply Element's answer")
		s.end(endFailed, "")
		return
	}
	_, err := s.ghost.SendMessage(s.ctx, s.portal.MXID, event.CallSelectAnswer, &event.Content{Parsed: &event.CallSelectAnswerEventContent{
		BaseCallEventContent: s.baseCallContent(),
		SelectedPartyID:      ans.PartyID,
	}}, nil)
	if err != nil {
		s.log.Warn().Err(err).Msg("Failed to send m.call.select_answer")
	}
	s.answerMessenger()
}

// end tears the call down once, telling whichever side didn't end it.
// notice, if set, is posted to the portal as the ghost.
func (s *callSession) end(reason endReason, detail string) {
	s.endOnce.Do(func() {
		s.log.Info().Int("end_reason", int(reason)).Msg("Ending call")
		s.lock.Lock()
		cc, joined := s.cc, s.joined
		conference, serverInfo := s.conference, s.serverInfoData
		metaLeg, mxLeg := s.metaLeg, s.mxLeg
		mxStarted := s.mxCallID != ""
		s.lock.Unlock()

		sendCtx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), 5*time.Second)
		defer cancel()

		// Messenger side.
		switch {
		case reason == endRemoteHangup || reason == endDismissed:
		case joined && cc != nil:
			hr := rtcsignal.HangupHangupCall
			if !s.incoming && reason == endTimeout {
				hr = rtcsignal.HangupNoAnswerTimeout
			}
			if _, err := s.cb.sig.Request(sendCtx, cc.NewHangup(hr)); err != nil {
				s.log.Warn().Err(err).Msg("Failed to send HANGUP to Messenger")
			}
		case reason == endLocalDecline && s.incoming && conference != "":
			// Declining a ring: HANGUP IGNORE_CALL on the ring's conference
			// (ZenonDismissReason.IgnoreCall).
			decline := rtcsignal.NewCallContext(strconv.FormatInt(s.m.selfFBID(), 10), conference, serverInfo)
			if _, err := s.cb.sig.Request(sendCtx, decline.NewHangup(rtcsignal.HangupIgnoreCall)); err != nil {
				s.log.Warn().Err(err).Msg("Failed to decline Messenger call")
			}
		}

		// Matrix side.
		if mxStarted && reason != endLocalHangup && reason != endLocalDecline {
			mxReason := event.CallHangupUserHangup
			switch reason {
			case endTimeout:
				mxReason = event.CallHangupInviteTimeout
			case endFailed, endBridgeShutdown:
				mxReason = event.CallHangupUnknownError
			case endDismissed:
				if detail != "" {
					mxReason = event.CallHangupReason(detail)
				}
				detail = ""
			}
			_, err := s.ghost.SendMessage(sendCtx, s.portal.MXID, event.CallHangup, &event.Content{Parsed: &event.CallHangupEventContent{
				BaseCallEventContent: s.baseCallContent(),
				Reason:               mxReason,
			}}, nil)
			if err != nil {
				s.log.Warn().Err(err).Msg("Failed to send m.call.hangup")
			} else {
				s.log.Info().Str("reason", string(mxReason)).Msg("Sent m.call.hangup")
			}
		}
		if detail != "" && reason == endFailed {
			_, _ = s.ghost.SendMessage(sendCtx, s.portal.MXID, event.EventMessage, &event.Content{Parsed: &event.MessageEventContent{
				MsgType: event.MsgNotice, Body: "Call bridging: " + detail,
			}}, nil)
		}

		s.cancel()
		metaLeg.Close()
		mxLeg.Close()
		s.cb.lock.Lock()
		if s.cb.active == s {
			s.cb.active = nil
		}
		s.cb.lock.Unlock()
	})
}
