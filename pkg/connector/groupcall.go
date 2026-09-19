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

// Group calls: Messenger runs them on its SFU ("MW" media mode). The bridge joins as one SFU client
// (the logged-in user), offering the web client's shape (audio sendrecv, video recvonly, data
// channel); the SFU answers in the JoinResponse and adds each participant's tracks later with delta
// SERVER_MEDIA_UPDATEs. On the Matrix side every Messenger participant is their own ghost in the
// room's MatrixRTC (LiveKit) call, publishing their audio and video, so the call looks like it does
// in Messenger. The Matrix user's audio goes up through the first participant's LiveKit leg, the only
// one that subscribes to them. See ~/proj/forks/messenger-sfu-spec.md for the protocol.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-meta/pkg/connector/callbridge"
	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

// collisionContextTopic is the app message (in RING and CONFERENCE_STATE) naming a group call's
// thread: JSON {group_thread_id, peer_id, server_info_data} (MultiwayCommonTypes
// getCollisionContextFromThriftAppMessages).
const collisionContextTopic = "collision_context_payload"

// groupThreadOf returns the group thread a call's app messages name, if any.
func groupThreadOf(msgs []rtcsignal.DataMessage) string {
	for _, m := range msgs {
		if m.Topic != collisionContextTopic && m.TopicDeprecated != collisionContextTopic {
			continue
		}
		var cc struct {
			GroupThreadID *string `json:"group_thread_id"`
		}
		if json.Unmarshal(m.Data, &cc) == nil && cc.GroupThreadID != nil {
			return *cc.GroupThreadID
		}
	}
	return ""
}

// isGroupRing reports whether a ring is for a group (SFU) call rather than a 1:1 one.
func isGroupRing(ring *rtcsignal.RingRequest) bool {
	return groupThreadOf(ring.AppMessages) != "" || len(ring.OtherParticipants) > 1 ||
		(ring.MediaPath == rtcsignal.MediaPathSFU && ring.UnifiedOffer == nil && ring.Offer == nil)
}

// groupParticipant is one Messenger participant's side in the Matrix call: their ghost's LiveKit leg.
type groupParticipant struct {
	userID string
	ghost  bridgev2.MatrixAPI
	rtc    *callbridge.RTCLeg
	joined bool       // call.member sent
	member id.EventID // the call.member event (ring notifications reference it)
	video  callbridge.RTPWriter
	tracks map[string]bool // Messenger track ids being relayed
}

// groupCall is a bridged Messenger group call (the bridge allows one call at a time, 1:1 or group).
type groupCall struct {
	cb       *callBridge
	m        *MetaClient
	ctx      context.Context
	cancel   context.CancelFunc
	log      zerolog.Logger
	portal   *bridgev2.Portal
	threadID string
	incoming bool
	e2ee     bool
	caller   string // incoming: who rang
	// peerID is set for a 1:1 call on the SFU (see moveToSFU): the other user of the DM.
	peerID string

	lock           sync.Mutex
	conference     string
	serverInfoData string
	cc             *rtcsignal.CallContext
	leg            *callbridge.Leg
	joining        bool
	joined         bool
	pendingCands   []rtcsignal.IceCandidate
	remoteCands    []webrtc.ICECandidateInit
	owners         map[string]string // Messenger track id -> owner user id (SMU media status)
	subscribed     map[string]bool   // video track ids with a TRACK subscription
	participants   map[string]*groupParticipant
	upstream       *groupParticipant // whose LiveKit leg takes the Matrix user's media
	focus          *rtcTransport
	createdTS      int64
	ring           id.EventID
	userPresent    bool
	crypt          *groupE2ee // end-to-end encryption, in an encrypted call

	endOnce sync.Once
}

func (cb *callBridge) newGroupCall(ctx context.Context, portal *bridgev2.Portal, threadID string, incoming bool) (*groupCall, error) {
	sctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	g := &groupCall{
		cb:           cb,
		m:            cb.m,
		ctx:          sctx,
		cancel:       cancel,
		portal:       portal,
		threadID:     threadID,
		incoming:     incoming,
		owners:       map[string]string{},
		subscribed:   map[string]bool{},
		participants: map[string]*groupParticipant{},
	}
	g.log = cb.log.With().
		Str("call_dir", map[bool]string{true: "incoming", false: "outgoing"}[incoming]).
		Str("group_call", threadID).
		Str("portal_id", string(portal.ID)).
		Logger()
	cb.lock.Lock()
	if cb.active != nil || cb.group != nil {
		cb.lock.Unlock()
		cancel()
		return nil, errBusy
	}
	cb.group = g
	cb.lock.Unlock()
	// markBridged takes cb.lock itself.
	cb.markBridged(portal.PortalKey)
	return g, nil
}

// findGroupPortal finds the portal of a group thread: a Messenger group, or an encrypted one.
func (cb *callBridge) findGroupPortal(ctx context.Context, threadID string) (*bridgev2.Portal, error) {
	keys := []networkid.PortalKey{
		{ID: networkid.PortalID(threadID)},
		{ID: networkid.PortalID(threadID), Receiver: cb.m.UserLogin.ID},
	}
	for _, key := range keys {
		portal, err := cb.m.Main.Bridge.GetExistingPortalByKey(ctx, key)
		if err != nil {
			return nil, err
		}
		if portal != nil && portal.MXID != "" {
			return portal, nil
		}
	}
	return nil, fmt.Errorf("no Matrix room for group thread %s", threadID)
}

func (g *groupCall) matches(msg *rtcsignal.Message) bool {
	g.lock.Lock()
	defer g.lock.Unlock()
	h := &msg.Header
	return (g.conference != "" && h.ConferenceName == g.conference) ||
		(g.serverInfoData != "" && h.ServerInfoData == g.serverInfoData)
}

// --- starting ---

// startIncomingGroup rings Matrix for a Messenger group call: the caller's ghost joins the room's
// call and rings the user; the user joining answers.
func (cb *callBridge) startIncomingGroup(ctx context.Context, msg *rtcsignal.Message) {
	ring := msg.Body.RingRequest
	log := cb.log.With().Str("conference", msg.Header.ConferenceName).Logger()
	threadID := groupThreadOf(ring.AppMessages)
	if threadID == "" && len(ring.OtherParticipants) <= 1 {
		cb.startIncomingSFU1to1(ctx, msg)
		return
	} else if threadID == "" {
		log.Info().Int("participants", len(ring.OtherParticipants)).Msg("Group call ring without a thread, not bridging")
		return
	}
	portal, err := cb.findGroupPortal(ctx, threadID)
	if err != nil {
		log.Info().Err(err).Msg("Not bridging group call")
		return
	}
	g, err := cb.newGroupCall(ctx, portal, threadID, true)
	if err != nil {
		log.Info().Err(err).Msg("Not bridging group call")
		return
	}
	g.lock.Lock()
	g.conference = msg.Header.ConferenceName
	g.serverInfoData = msg.Header.ServerInfoData
	g.caller = ring.Caller
	g.e2ee = ring.E2eeEnforcement == nil || ring.E2eeEnforcement.Mode == rtcsignal.E2eeMandated
	g.lock.Unlock()
	g.log.Info().Str("caller", ring.Caller).Int("participants", len(ring.OtherParticipants)).Bool("e2ee", g.e2ee).
		Int32("media_path", int32(ring.MediaPath)).Msg("Ringing Matrix for incoming Messenger group call")
	if !g.canBridgeMedia() {
		return
	}
	if err = g.ringMatrix(ring.Caller); err != nil {
		g.log.Err(err).Msg("Failed to ring Matrix for the group call")
		g.end("")
		return
	}
	time.AfterFunc(rtcRingLifetime, func() {
		g.lock.Lock()
		joined := g.joined || g.joining
		g.lock.Unlock()
		if !joined && g.ctx.Err() == nil {
			g.log.Info().Msg("Nobody answered the group call in Matrix")
			g.end("")
		}
	})
}

// startOutgoingGroup places a Messenger group call when the user starts a MatrixRTC call in a group
// portal: the bridge joins the SFU and the server rings the thread's members.
func (cb *callBridge) startOutgoingGroup(ctx context.Context, portal *bridgev2.Portal) {
	threadID := string(portal.ID)
	g, err := cb.newGroupCall(ctx, portal, threadID, false)
	if err != nil {
		zerolog.Ctx(ctx).Info().Err(err).Msg("Not bridging group call")
		return
	}
	meta, _ := portal.Metadata.(*metaid.PortalMetadata)
	g.lock.Lock()
	g.e2ee = meta != nil && meta.WhatsAppServer != ""
	g.lock.Unlock()
	g.log.Info().Bool("e2ee", g.e2ee).Msg("Outgoing Messenger group call from Matrix")
	if !g.canBridgeMedia() {
		return
	}
	members, err := g.threadMembers()
	if err != nil || len(members) == 0 {
		g.log.Warn().Err(err).Msg("No Messenger members to call in the group")
		g.end("The bridge couldn't find anyone to call in this group")
		return
	}
	go g.joinMessenger(members)
}

// canBridgeMedia checks that the call's media can be bridged: an end-to-end encrypted group call
// needs the bridge's Messenger encryption device and Meta's frame-encryption module, which starts
// loading here (it's needed for the JOIN).
func (g *groupCall) canBridgeMedia() bool {
	if g.m.callIdentity() == nil && g.e2ee {
		g.log.Info().Msg("End-to-end encrypted group call, but the bridge has no Messenger encryption device")
		g.end("The bridge has no Messenger encryption device, so it can't join this encrypted call")
		return false
	}
	go func() {
		if _, err := g.m.frameCryptRuntime(g.ctx, g.roomID()); err != nil {
			g.log.Err(err).Msg("Failed to load the frame-encryption module")
		}
	}()
	return true
}

// threadMembers lists the Messenger user ids of the portal's ghosts (everyone but the user).
func (g *groupCall) threadMembers() ([]string, error) {
	members, err := g.m.Main.Bridge.Matrix.GetMembers(g.ctx, g.portal.MXID)
	if err != nil {
		return nil, err
	}
	self := strconv.FormatInt(g.m.selfFBID(), 10)
	var out []string
	for mxid, mem := range members {
		if mem.Membership != event.MembershipJoin {
			continue
		}
		uid, ok := g.m.Main.Bridge.Matrix.ParseGhostMXID(mxid)
		if !ok || string(uid) == self {
			continue
		}
		out = append(out, string(uid))
	}
	return out, nil
}

// --- Matrix side ---

func (g *groupCall) participant(userID string) (*groupParticipant, error) {
	g.lock.Lock()
	p := g.participants[userID]
	g.lock.Unlock()
	if p != nil {
		return p, nil
	}
	ghost, err := g.m.Main.Bridge.GetGhostByID(g.ctx, networkid.UserID(userID))
	if err != nil || ghost == nil {
		return nil, fmt.Errorf("ghost for %s: %w", userID, err)
	}
	p = &groupParticipant{userID: userID, ghost: ghost.Intent, tracks: map[string]bool{}}
	g.lock.Lock()
	if existing := g.participants[userID]; existing != nil {
		p = existing
	} else {
		g.participants[userID] = p
	}
	g.lock.Unlock()
	return p, nil
}

func ghostClient(api bridgev2.MatrixAPI) (*mautrix.Client, error) {
	gi, ok := api.(*matrix.ASIntent)
	if !ok {
		return nil, errors.New("ghost intent isn't an appservice intent")
	}
	return gi.Matrix.Client, nil
}

// joinRTC puts a participant's ghost into the room's LiveKit call with a call.member. The first one
// becomes the upstream leg, the only one subscribing to the Matrix user.
func (g *groupCall) joinRTC(p *groupParticipant) error {
	g.lock.Lock()
	if p.rtc != nil {
		g.lock.Unlock()
		return nil
	}
	upstream := g.upstream == nil
	if upstream {
		g.upstream = p
	}
	g.lock.Unlock()
	focus, err := g.m.Main.liveKitFocus(g.ctx)
	if err != nil {
		return err
	}
	cli, err := ghostClient(p.ghost)
	if err != nil {
		return err
	}
	url, token, err := liveKitToken(g.ctx, cli, focus, g.portal.MXID, rtcDeviceID)
	if err != nil {
		return err
	}
	isUser := func(identity string) bool {
		return strings.HasPrefix(identity, string(g.m.UserLogin.UserMXID)+":")
	}
	accept := func(string) bool { return false }
	if upstream {
		accept = isUser
	}
	leg, err := callbridge.JoinRTC(g.ctx, callbridge.RTCLegConfig{
		URL: url, Token: token, Accept: accept,
		Log: g.log.With().Str("participant", p.userID).Logger(),
	})
	if err != nil {
		return err
	}
	g.lock.Lock()
	p.rtc = leg
	g.focus = focus
	if g.createdTS == 0 {
		g.createdTS = time.Now().UnixMilli()
	}
	created := g.createdTS
	g.lock.Unlock()
	if upstream {
		leg.OnPeers(g.onRTCPeers)
		go g.relayUserAudio(leg)
	}
	resp, err := cli.SendStateEvent(g.ctx, g.portal.MXID, evtCallMember, rtcStateKey(cli.UserID, rtcDeviceID),
		rtcMemberContent(focus, g.portal.MXID, rtcDeviceID, string(cli.UserID), false, created))
	if err != nil {
		return fmt.Errorf("send call membership: %w", err)
	}
	g.lock.Lock()
	p.joined = true
	p.member = resp.EventID
	g.lock.Unlock()
	return nil
}

// leaveRTC clears a participant's call.member and leaves LiveKit (the upstream leg stays connected,
// without a membership, so the user's media keeps flowing to Messenger).
func (g *groupCall) leaveRTC(ctx context.Context, p *groupParticipant, final bool) {
	g.lock.Lock()
	joined, leg := p.joined, p.rtc
	p.joined = false
	isUpstream := g.upstream == p
	if !isUpstream || final {
		p.rtc = nil
	}
	g.lock.Unlock()
	if joined {
		if cli, err := ghostClient(p.ghost); err == nil {
			_, _ = cli.SendStateEvent(ctx, g.portal.MXID, evtCallMember, rtcStateKey(cli.UserID, rtcDeviceID), map[string]any{})
		}
	}
	if leg != nil && (!isUpstream || final) {
		leg.Close()
	}
}

// ringMatrix has the caller's ghost join the call and ring the user.
func (g *groupCall) ringMatrix(caller string) error {
	if err := g.m.Main.ensureCallPowerLevel(g.ctx, g.portal.MXID); err != nil {
		g.log.Warn().Err(err).Msg("Couldn't allow call memberships in the portal")
	}
	p, err := g.participant(caller)
	if err != nil {
		return err
	}
	if err = g.joinRTC(p); err != nil {
		return fmt.Errorf("join MatrixRTC call: %w", err)
	}
	g.lock.Lock()
	member := p.member
	g.lock.Unlock()
	// Element X only rings for a notification that references the ringing member's call.member.
	resp, err := p.ghost.SendMessage(g.ctx, g.portal.MXID, evtRTCNotification, &event.Content{Raw: map[string]any{
		"notification_type": "ring",
		"m.mentions":        map[string]any{"user_ids": []id.UserID{g.m.UserLogin.UserMXID}},
		"m.relates_to":      map[string]any{"rel_type": "m.reference", "event_id": member},
		"sender_ts":         time.Now().UnixMilli(),
		"lifetime":          rtcRingLifetime.Milliseconds(),
		"m.call.intent":     "audio",
	}}, nil)
	if err != nil {
		return fmt.Errorf("send ring: %w", err)
	}
	g.lock.Lock()
	g.ring = resp.EventID
	g.lock.Unlock()
	g.log.Info().Stringer("ring_event", resp.EventID).Stringer("member_event", member).Msg("Rang Matrix for the group call")
	return nil
}

// onRTCPeers joins Messenger once the user is in the Matrix call, and ends the call when they leave.
func (g *groupCall) onRTCPeers(identities []string) {
	present := false
	for _, ident := range identities {
		if strings.HasPrefix(ident, string(g.m.UserLogin.UserMXID)+":") {
			present = true
			break
		}
	}
	g.lock.Lock()
	was := g.userPresent
	g.userPresent = present
	g.lock.Unlock()
	switch {
	case present && !was:
		g.userJoined()
	case !present && was:
		g.log.Info().Msg("The Matrix user left the group call")
		go g.end("")
	}
}

// userJoined answers an incoming group call.
func (g *groupCall) userJoined() {
	if !g.incoming {
		return
	}
	g.lock.Lock()
	already := g.joining || g.joined
	g.joining = true
	g.lock.Unlock()
	if !already {
		g.log.Info().Msg("Answered the group call in Matrix")
		go g.joinMessenger(nil)
	}
}

// relayUserAudio sends the Matrix user's audio (taken by the upstream leg) to Messenger.
func (g *groupCall) relayUserAudio(rtc *callbridge.RTCLeg) {
	tr, err := rtc.RemoteAudio(g.ctx)
	if err != nil {
		return
	}
	for g.ctx.Err() == nil {
		g.lock.Lock()
		// Only once joined: before that an encrypted call's encryption may not be set up yet.
		leg, crypt, joined := g.leg, g.crypt, g.joined
		g.lock.Unlock()
		if leg != nil && joined {
			log := g.log.With().Str("from", "matrixrtc").Logger()
			if crypt != nil {
				var stats callbridge.FrameRelayStats
				err = callbridge.RelayAudioTransformed(g.ctx, tr, uint8(tr.PayloadType()), leg.Local, crypt.encryptTransform(log), &stats, log)
				g.log.Info().AnErr("relay_err", err).Uint64("forwarded", stats.Forwarded.Load()).
					Uint64("frames", stats.Frames.Load()).Uint64("failed", stats.Failed.Load()).Msg("Matrix audio relay stopped")
				return
			}
			var stats callbridge.RelayStats
			err = callbridge.Relay(g.ctx, tr, uint8(tr.PayloadType()), leg.Local, &stats, log)
			g.log.Info().AnErr("relay_err", err).Uint64("forwarded", stats.Forwarded.Load()).Msg("Matrix audio relay stopped")
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// --- Messenger side ---

// joinMessenger joins the group call on Messenger's SFU (usersToCall rings them for an outgoing call).
func (g *groupCall) joinMessenger(usersToCall []string) {
	id := g.m.callIdentity()
	if id == nil {
		g.end("The bridge has no Messenger encryption device, so it can't join calls")
		return
	}
	leg, err := callbridge.NewLeg(callbridge.LegConfig{
		Name: "messenger-sfu", WebShape: true, SFU: true, AllowVideo: true, Log: g.log,
	})
	if err != nil {
		g.log.Err(err).Msg("Failed to create the Messenger SFU connection")
		g.end("")
		return
	}
	leg.OnRemoteTrack(g.onRemoteTrack)
	leg.OnCandidate(g.onLocalCandidate)
	g.lock.Lock()
	g.leg = leg
	pending := g.remoteCands
	g.remoteCands = nil
	g.lock.Unlock()
	for _, c := range pending {
		_ = leg.AddCandidate(c)
	}
	offer, err := leg.CreateOffer()
	if err == nil {
		offer, err = callbridge.PrepareMetaLocalSDP(offer, id, false)
	}
	if err != nil {
		g.log.Err(err).Msg("Failed to create the SFU offer")
		g.end("")
		return
	}
	g.lock.Lock()
	cc := rtcsignal.NewCallContext(strconv.FormatInt(g.m.selfFBID(), 10), g.conference, g.serverInfoData)
	e2ee := g.e2ee
	g.lock.Unlock()
	// Frame encryption runs in every SFU call, as in the web client: Messenger negotiates it on
	// for unencrypted chats too when all clients support it.
	e2eeState := g.m.callE2eeState()
	var crypt *groupE2ee
	if state, err := g.startE2ee(offer); err != nil && e2ee {
		g.log.Err(err).Msg("Failed to set up end-to-end encryption for the group call")
		g.end("The bridge couldn't set up end-to-end encryption for this Messenger group call")
		return
	} else if err != nil {
		g.log.Warn().Err(err).Msg("Failed to set up frame encryption, joining without it")
	} else {
		e2eeState = state
		g.lock.Lock()
		crypt = g.crypt
		g.lock.Unlock()
	}
	resp, err := g.cb.sig.RequestHook(g.ctx, cc.NewJoin(&rtcsignal.JoinParams{
		Offer:         offer,
		SFU:           true,
		GroupThreadID: g.threadID,
		PeerID:        g.peerID,
		UsersToCall:   usersToCall,
		AudioTrackID:  leg.TrackID,
		E2eeState:     e2eeState,
		E2eeMandated:  e2ee,
	}), func(resp *rtcsignal.Message) {
		if resp.Header.ResponseStatusCode != rtcsignal.StatusOK {
			return
		}
		g.lock.Lock()
		if cc.ConferenceName == "" {
			cc.ConferenceName = resp.Header.ConferenceName
			cc.ServerInfoData = resp.Header.ServerInfoData
			g.conference = resp.Header.ConferenceName
			g.serverInfoData = resp.Header.ServerInfoData
		}
		g.cc = cc
		g.lock.Unlock()
		// In the hook, so the server state reaches the module before any later media update's.
		if crypt != nil && resp.Body.JoinResponse != nil {
			crypt.serverState(resp.Body.JoinResponse.StateStore, "join")
		}
	})
	if err != nil {
		g.log.Err(err).Msg("Group call JOIN failed")
		g.end("Couldn't join the Messenger group call")
		return
	}
	jr := resp.Body.JoinResponse
	if jr == nil || jr.Answer == nil || jr.Answer.SDP == "" {
		g.log.Error().Bool("has_body", jr != nil).Msg("Group call JOIN response without an answer")
		g.end("")
		return
	}
	g.log.Info().Int32("media_path", int32(jr.MediaPath)).Int64("sctp_node", jr.SelfSCTPNodeID).
		Int("groups_of_users", len(jr.GroupsOfUsers)).Msg("Joined Messenger group call")
	callbridge.LogSDPShape(g.log.Info(), jr.Answer.SDP).Msg("SFU answer")
	g.log.Debug().Str("offer", callbridge.RedactSDP(offer)).Str("answer", callbridge.RedactSDP(jr.Answer.SDP)).Msg("SFU JOIN SDPs")
	if err = leg.SetAnswer(callbridge.PrepareMetaRemoteSDP(jr.Answer.SDP)); err != nil {
		g.log.Err(err).Msg("Failed to apply the SFU's answer")
		g.end("")
		return
	}
	g.lock.Lock()
	g.joined = true
	cands := g.pendingCands
	g.pendingCands = nil
	g.lock.Unlock()
	for _, c := range cands {
		go g.request(cc.NewIceCandidates(c))
	}
	go g.request(cc.NewDominantSpeakerSubscription())
}

func (g *groupCall) request(msg *rtcsignal.Message) {
	if _, err := g.cb.sig.Request(g.ctx, msg); err != nil && g.ctx.Err() == nil {
		g.log.Debug().Err(err).Stringer("type", msg.Header.Type).Msg("Group call request failed")
	}
}

func (g *groupCall) onLocalCandidate(c *webrtc.ICECandidateInit) {
	ic := rtcsignal.IceCandidate{Candidate: c.Candidate}
	if c.SDPMid != nil {
		ic.SDPMid = *c.SDPMid
	}
	if c.SDPMLineIndex != nil {
		ic.SDPMLineIndex = int64(*c.SDPMLineIndex)
	}
	g.lock.Lock()
	if !g.joined {
		g.pendingCands = append(g.pendingCands, ic)
		g.lock.Unlock()
		return
	}
	cc := g.cc
	g.lock.Unlock()
	go g.request(cc.NewIceCandidates(ic))
}

func (g *groupCall) respond(msg *rtcsignal.Message, body rtcsignal.Body) *rtcsignal.Message {
	g.lock.Lock()
	cc := g.cc
	g.lock.Unlock()
	if cc == nil {
		return nil
	}
	return cc.Respond(msg, body)
}

func (g *groupCall) handleSignal(msg *rtcsignal.Message) *rtcsignal.Message {
	b := &msg.Body
	switch {
	case b.IceCandidateRequest != nil:
		for _, c := range b.IceCandidateRequest.Candidates {
			idx := uint16(c.SDPMLineIndex)
			mid := c.SDPMid
			init := webrtc.ICECandidateInit{Candidate: c.Candidate, SDPMid: &mid, SDPMLineIndex: &idx}
			g.lock.Lock()
			leg := g.leg
			if leg == nil {
				g.remoteCands = append(g.remoteCands, init)
			}
			g.lock.Unlock()
			if leg != nil {
				_ = leg.AddCandidate(init)
			}
		}
	case b.ServerMediaUpdateRequest != nil:
		return g.handleServerMediaUpdate(msg)
	case b.ConferenceStateRequest != nil:
		g.handleConferenceState(b.ConferenceStateRequest)
	case b.HangupRequest != nil:
		g.log.Info().Int32("reason", int32(b.HangupRequest.Reason)).Msg("Messenger ended the group call for us")
		go g.end("")
	case b.DismissRequest != nil:
		g.lock.Lock()
		joined := g.joined || g.joining
		g.lock.Unlock()
		if !joined {
			g.log.Info().Int32("reason", int32(b.DismissRequest.Reason)).Msg("Group call ringing dismissed")
			go g.end("")
		}
	case b.DataMessageRequest != nil && b.DataMessageRequest.Message != nil:
		dm := b.DataMessageRequest.Message
		g.lock.Lock()
		crypt := g.crypt
		g.lock.Unlock()
		if dm.Topic == rtcsignal.TopicE2eeKey && crypt != nil {
			crypt.keyMessage(dm)
		} else {
			g.log.Info().Str("topic", dm.Topic).Str("topic_deprecated", dm.TopicDeprecated).Str("sender", dm.Sender).
				Str("sender_e2ee_id", dm.SenderE2eeID).Int("data_len", len(dm.Data)).Int("e2e_encrypted_len", len(dm.E2eEncryptedData)).
				Bool("other_body", dm.HasOtherBodyMember).Msg("Group call data message")
		}
	}
	return g.respond(msg, rtcsignal.DefaultResponseBody(msg))
}

// handleServerMediaUpdate applies the SFU's updates: a delta (participants' m-sections added or
// changed) or a full offer is answered in the response, and the media status says who owns which
// track, which picks the ghost their media goes to.
func (g *groupCall) handleServerMediaUpdate(msg *rtcsignal.Message) *rtcsignal.Message {
	smu := msg.Body.ServerMediaUpdateRequest
	g.lock.Lock()
	leg, crypt := g.leg, g.crypt
	for trackID, ti := range smu.MediaStatus {
		if ti.Owner != "" {
			g.owners[trackID] = ti.Owner
		}
	}
	g.lock.Unlock()
	if crypt != nil {
		crypt.serverState(smu.StateStore, "media_update")
	}
	resp := &rtcsignal.ServerMediaUpdateResponse{CurrentVersion: smu.ToVersion}
	if leg == nil {
		return g.respond(msg, rtcsignal.Body{ServerMediaUpdateResponse: resp})
	}
	var answer string
	var err error
	switch {
	case smu.Update != nil:
		answer, err = leg.AnswerDelta(smu.Update)
	case smu.Offer != nil || smu.RenegotiationOffer != nil:
		_, sd := smu.RemoteSDP()
		answer, err = leg.AnswerRenegotiation(callbridge.PrepareMetaRemoteSDP(sd.SDP))
	case smu.Answer != nil:
		err = leg.SetRenegotiationAnswer(callbridge.PrepareMetaRemoteSDP(smu.Answer.SDP))
	}
	if g.log.GetLevel() <= zerolog.DebugLevel {
		ev := g.log.Debug().Bool("offer", smu.Offer != nil).Bool("renegotiation_offer", smu.RenegotiationOffer != nil).
			Bool("answer", smu.Answer != nil).Bool("renegotiation_requested", smu.RenegotiationRequested)
		if smu.Update != nil {
			for _, idx := range smu.Update.Indexes() {
				m := smu.Update.Media[idx]
				ev = ev.Str("delta_"+strconv.Itoa(int(idx)), "mid="+m.MID+" msid="+m.MSID+"\n"+callbridge.RedactSDP(m.Body))
			}
		}
		for trackID, ti := range smu.MediaStatus {
			ev = ev.Str("track_"+trackID, fmt.Sprintf("owner=%s label=%d enabled=%v", ti.Owner, ti.Label, ti.Enabled))
		}
		ev.Msg("Group call media update contents")
	}
	g.log.Info().Int64("from_version", smu.FromVersion).Int64("to_version", smu.ToVersion).
		Ints32("tags", msg.Header.MessageTags).Bool("delta", smu.Update != nil).Int("owners", len(smu.MediaStatus)).
		AnErr("err", err).Msg("Group call media update")
	if err != nil {
		return g.respond(msg, rtcsignal.Body{ServerMediaUpdateResponse: resp})
	}
	if answer != "" {
		if id := g.m.callIdentity(); id != nil {
			if signed, err := callbridge.PrepareMetaLocalSDP(answer, id, false); err == nil {
				answer = signed
			}
		}
		resp.Answer = &rtcsignal.SessionDescription{SDP: answer}
	}
	go g.updateSubscriptions(smu.MediaStatus)
	return g.respond(msg, rtcsignal.Body{ServerMediaUpdateResponse: resp})
}

// updateSubscriptions asks the SFU for every remote video track (tweb/web client: TRACK subscriptions
// at MEDIUM quality; audio flows without one), keeping the dominant-speaker subscription.
func (g *groupCall) updateSubscriptions(status map[string]rtcsignal.TrackInfo) {
	g.lock.Lock()
	cc := g.cc
	self := strconv.FormatInt(g.m.selfFBID(), 10)
	changed := false
	for trackID, ti := range status {
		if ti.Label != rtcsignal.TrackLabelVideo || ti.Owner == self || !ti.Enabled || g.subscribed[trackID] {
			continue
		}
		g.subscribed[trackID] = true
		changed = true
	}
	subs := []rtcsignal.Subscription{{Type: 2}}
	for trackID := range g.subscribed {
		subs = append(subs, rtcsignal.Subscription{Type: 1, TrackID: trackID, VideoQuality: 1})
	}
	g.lock.Unlock()
	if !changed || cc == nil {
		return
	}
	g.request(cc.Request(rtcsignal.TypeSubscription, rtcsignal.Body{SubscriptionRequest: &rtcsignal.SubscriptionRequest{Subscriptions: subs}}))
}

func (g *groupCall) handleConferenceState(cs *rtcsignal.ConferenceStateRequest) {
	self := strconv.FormatInt(g.m.selfFBID(), 10)
	for userID, ps := range cs.ParticipantStates {
		if userID == self {
			continue
		}
		g.log.Debug().Str("user", userID).Int32("state", int32(ps.State)).Msg("Group call participant state")
		if userID == g.peerID {
			switch ps.State {
			case rtcsignal.StateDisconnected, rtcsignal.StateConnectionDropped, rtcsignal.StateRejected,
				rtcsignal.StateNoAnswer, rtcsignal.StateUnreachable, rtcsignal.StateInAnotherCall:
				g.log.Info().Int32("state", int32(ps.State)).Msg("The other person left the 1:1 call")
				go g.end("")
				return
			}
		}
		if ps.State == rtcsignal.StateDisconnected || ps.State == rtcsignal.StateConnectionDropped {
			g.lock.Lock()
			p := g.participants[userID]
			g.lock.Unlock()
			if p != nil {
				go g.leaveRTC(g.ctx, p, false)
			}
		}
	}
}

// onRemoteTrack relays a participant's track to their ghost in the Matrix call.
func (g *groupCall) onRemoteTrack(tr *webrtc.TrackRemote) {
	g.lock.Lock()
	owner := g.owners[tr.ID()]
	g.lock.Unlock()
	if owner == "" {
		owner, _, _ = strings.Cut(tr.StreamID(), ":")
	}
	log := g.log.With().Str("owner", owner).Stringer("kind", tr.Kind()).Str("track", tr.ID()).Uint32("ssrc", uint32(tr.SSRC())).Logger()
	log.Info().Str("stream", tr.StreamID()).Msg("Group call track")
	if owner == "" || owner == strconv.FormatInt(g.m.selfFBID(), 10) {
		log.Info().Str("track", tr.ID()).Str("stream", tr.StreamID()).Str("rid", tr.RID()).Uint32("ssrc", uint32(tr.SSRC())).
			Uint8("pt", uint8(tr.PayloadType())).Msg("Ignoring a group call track without another owner")
		return
	}
	p, err := g.participant(owner)
	if err == nil {
		err = g.joinRTC(p)
	}
	if err != nil {
		log.Err(err).Msg("Failed to put a group call participant into the Matrix call")
		return
	}
	g.lock.Lock()
	rtc := p.rtc
	p.tracks[tr.ID()] = true
	crypt := g.crypt
	g.lock.Unlock()
	if crypt != nil {
		g.relayDecrypted(crypt, p, rtc, tr, log)
		return
	}
	var stats callbridge.RelayStats
	if tr.Kind() == webrtc.RTPCodecTypeVideo {
		g.lock.Lock()
		w := p.video
		g.lock.Unlock()
		if w == nil {
			if w, err = rtc.AddVideoTrack(tr.Codec().MimeType); err != nil {
				log.Err(err).Msg("Failed to publish a participant's video")
				return
			}
			g.lock.Lock()
			p.video = w
			g.lock.Unlock()
		}
		g.forwardKeyframes(rtc, tr)
		err = callbridge.RelayVideo(g.ctx, tr, uint8(tr.PayloadType()), w, &stats, log)
	} else {
		err = callbridge.Relay(g.ctx, tr, uint8(tr.PayloadType()), rtc.AudioWriter(), &stats, log)
	}
	log.Info().AnErr("relay_err", err).Uint64("forwarded", stats.Forwarded.Load()).Msg("Participant relay stopped")
}

// forwardKeyframes asks Messenger for keyframes of a participant's video: a few right away (nobody
// in the Matrix call can show anything before one) and whenever the participant's LiveKit leg is
// asked for one, at most every 500 ms.
func (g *groupCall) forwardKeyframes(rtc *callbridge.RTCLeg, tr *webrtc.TrackRemote) {
	g.lock.Lock()
	leg := g.leg
	g.lock.Unlock()
	if leg == nil {
		return
	}
	ssrc := tr.SSRC()
	var mu sync.Mutex
	var last time.Time
	rtc.OnKeyframeRequest(func() {
		mu.Lock()
		if time.Since(last) < 500*time.Millisecond {
			mu.Unlock()
			return
		}
		last = time.Now()
		mu.Unlock()
		leg.RequestKeyframe(ssrc)
	})
	go func() {
		for _, d := range []time.Duration{0, 300 * time.Millisecond, 700 * time.Millisecond, time.Second} {
			select {
			case <-g.ctx.Done():
				return
			case <-time.After(d):
			}
			leg.RequestKeyframe(ssrc)
		}
	}()
}

// relayDecrypted relays an encrypted call's track: every frame is decrypted with the sender's key
// (the sender is named by the track's msid, "<userId>:<cname>:<streamId>").
func (g *groupCall) relayDecrypted(crypt *groupE2ee, p *groupParticipant, rtc *callbridge.RTCLeg, tr *webrtc.TrackRemote, log zerolog.Logger) {
	e2eeID := callbridge.E2eeIDOfStream(tr.StreamID())
	if e2eeID == "" {
		log.Warn().Str("stream", tr.StreamID()).Msg("Can't tell who sent an encrypted track, not relaying it")
		return
	}
	video := tr.Kind() == webrtc.RTPCodecTypeVideo
	xf, closeFn, err := crypt.decryptor(e2eeID, !video, log)
	if err != nil {
		log.Err(err).Msg("Failed to create a frame decryptor")
		return
	}
	defer closeFn()
	var stats callbridge.FrameRelayStats
	if video {
		g.lock.Lock()
		w := p.video
		g.lock.Unlock()
		if w == nil {
			if w, err = rtc.AddVideoTrack(tr.Codec().MimeType); err != nil {
				log.Err(err).Msg("Failed to publish a participant's video")
				return
			}
			g.lock.Lock()
			p.video = w
			g.lock.Unlock()
		}
		g.lock.Lock()
		leg := g.leg
		g.lock.Unlock()
		onLoss := func() {
			if leg != nil {
				leg.RequestKeyframe(tr.SSRC())
			}
		}
		g.forwardKeyframes(rtc, tr)
		err = callbridge.RelayVideoTransformed(g.ctx, tr, uint8(tr.PayloadType()), tr.Codec().MimeType, w, xf, onLoss, &stats, log)
	} else {
		err = callbridge.RelayAudioTransformed(g.ctx, tr, uint8(tr.PayloadType()), rtc.AudioWriter(), xf, &stats, log)
	}
	log.Info().AnErr("relay_err", err).Uint64("forwarded", stats.Forwarded.Load()).Uint64("frames", stats.Frames.Load()).
		Uint64("failed", stats.Failed.Load()).Uint64("lost_packets", stats.LostPackets.Load()).Msg("Participant relay stopped")
}

// --- ending ---

// end leaves both sides; notice, if set, is posted to the room as the reason.
func (g *groupCall) end(notice string) {
	g.endOnce.Do(func() {
		g.log.Info().Str("notice", notice).Msg("Group call ended")
		ctx, cancel := context.WithTimeout(context.WithoutCancel(g.ctx), 20*time.Second)
		defer cancel()
		g.lock.Lock()
		cc, leg, joined := g.cc, g.leg, g.joined
		parts := make([]*groupParticipant, 0, len(g.participants))
		for _, p := range g.participants {
			parts = append(parts, p)
		}
		g.lock.Unlock()
		if joined && cc != nil {
			if _, err := g.cb.sig.Request(ctx, cc.NewHangup(rtcsignal.HangupHangupCall)); err != nil {
				g.log.Debug().Err(err).Msg("Group call HANGUP failed")
			}
		}
		for _, p := range parts {
			g.leaveRTC(ctx, p, true)
		}
		if leg != nil {
			leg.Close()
		}
		g.cancel()
		g.lock.Lock()
		crypt := g.crypt
		g.lock.Unlock()
		if crypt != nil {
			crypt.close()
		}
		g.cb.lock.Lock()
		if g.cb.group == g {
			g.cb.group = nil
		}
		g.cb.lock.Unlock()
		g.cb.markBridged(g.portal.PortalKey)
		if notice != "" {
			_, _ = g.m.Main.Bridge.Bot.SendMessage(ctx, g.portal.MXID, event.EventMessage, &event.Content{
				Parsed: &event.MessageEventContent{MsgType: event.MsgNotice, Body: notice},
			}, nil)
		}
	})
}

// isGroupPortal reports whether a portal is a group chat (calls there are group calls).
func isGroupPortal(portal *bridgev2.Portal) bool {
	return portal.RoomType != database.RoomTypeDM
}

// --- 1:1 calls on the SFU ---

// roomID is the id of the call's Messenger room page (/groupcall/ROOM:<id>/): the group thread, or
// the peer of a 1:1 call.
func (g *groupCall) roomID() string {
	if g.threadID != "" {
		return g.threadID
	}
	return g.peerID
}

// moveToSFU carries on a 1:1 MatrixRTC call that Messenger put on its SFU (it does so for some calls
// despite preventSFUMode, and when a call becomes a group call) as a group call with one other
// participant: the session ends quietly (no HANGUP, and the user stays in the Matrix call) and a
// group call joins the same conference as an SFU client. It reports false for a call it can't carry
// on (legacy m.call.* calls: the group path needs MatrixRTC), which the caller then ends.
func (cb *callBridge) moveToSFU(s *callSession) bool {
	s.lock.Lock()
	rtcMode, conference, serverInfo, e2ee := s.rtcMode, s.conference, s.serverInfoData, s.e2ee
	s.lock.Unlock()
	if !rtcMode || conference == "" {
		return false
	}
	s.log.Info().Bool("e2ee", e2ee).Msg("Messenger put the call on its SFU, carrying it on as a group call")
	s.end(endMovedToSFU, "")
	peer := strconv.FormatInt(s.peerID, 10)
	g, err := cb.newGroupCall(s.ctx, s.portal, "", s.incoming)
	if err != nil {
		s.log.Err(err).Msg("Couldn't carry the call on on the SFU")
		return true
	}
	g.lock.Lock()
	g.peerID = peer
	g.conference = conference
	g.serverInfoData = serverInfo
	g.e2ee = e2ee
	g.joining = true
	g.lock.Unlock()
	g.log = g.log.With().Str("peer", peer).Str("conference", conference).Logger()
	if !g.canBridgeMedia() {
		return true
	}
	go func() {
		p, err := g.participant(peer)
		if err == nil {
			err = g.joinRTC(p)
		}
		if err != nil {
			g.log.Err(err).Msg("Failed to put the other person back into the Matrix call")
			g.end("")
			return
		}
		g.joinMessenger(nil)
	}()
	return true
}

// startIncomingSFU1to1 rings Matrix for a 1:1 call that Messenger rings on its SFU (no offer, no group
// thread): the caller's ghost joins the DM's call, like for a group call.
func (cb *callBridge) startIncomingSFU1to1(ctx context.Context, msg *rtcsignal.Message) {
	ring := msg.Body.RingRequest
	log := cb.log.With().Str("conference", msg.Header.ConferenceName).Str("caller", ring.Caller).Logger()
	caller, err := strconv.ParseInt(ring.Caller, 10, 64)
	if err != nil || caller == 0 || caller == cb.m.selfFBID() {
		log.Info().Msg("1:1 SFU ring without another caller, not bridging")
		return
	}
	if !cb.m.matrixRTCEnabled() {
		log.Info().Msg("1:1 SFU ring, but MatrixRTC calls are off, not bridging")
		return
	}
	portal, err := cb.findDMPortal(ctx, caller)
	if err != nil {
		log.Info().Err(err).Msg("Not bridging 1:1 SFU call")
		return
	}
	g, err := cb.newGroupCall(ctx, portal, "", true)
	if err != nil {
		log.Info().Err(err).Msg("Not bridging 1:1 SFU call")
		return
	}
	g.lock.Lock()
	g.peerID = ring.Caller
	g.conference = msg.Header.ConferenceName
	g.serverInfoData = msg.Header.ServerInfoData
	g.caller = ring.Caller
	g.e2ee = ring.E2eeEnforcement == nil || ring.E2eeEnforcement.Mode == rtcsignal.E2eeMandated
	g.lock.Unlock()
	g.log.Info().Bool("e2ee", g.e2ee).Int32("media_path", int32(ring.MediaPath)).Msg("Ringing Matrix for an incoming 1:1 call on Messenger's SFU")
	if !g.canBridgeMedia() {
		return
	}
	if err = g.ringMatrix(ring.Caller); err != nil {
		g.log.Err(err).Msg("Failed to ring Matrix for the call")
		g.end("")
		return
	}
	time.AfterFunc(rtcRingLifetime, func() {
		g.lock.Lock()
		joined := g.joined || g.joining
		g.lock.Unlock()
		if !joined && g.ctx.Err() == nil {
			g.log.Info().Msg("Nobody answered the call in Matrix")
			g.end("")
		}
	})
}
