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

// MatrixRTC (Element Call / Element X) call bridging. Element X only calls through MatrixRTC: every
// participant publishes an org.matrix.msc3401.call.member state event and sends media through the
// homeserver's LiveKit. The bridge takes part the same way, as the Messenger user's ghost: it joins
// the room's LiveKit call (callbridge.RTCLeg) and relays the audio/video to the Messenger leg.
//
// Wire formats, as Element X (matrix-rust-sdk / ruma) and Element Call (matrix-js-sdk) use them:
//   - membership: the legacy per-device state event (the sticky org.matrix.msc4143.rtc.member form is
//     invisible to Element X), state key "_@user:server_DEVICE_m.call", content below; {} = left.
//   - LiveKit token: lk-jwt-service /sfu/get, whose identity "@user:server:DEVICE" is what Element Call
//     matches to the membership (sender:device_id).
//   - ring: org.matrix.msc4075.rtc.notification, notification_type "ring", referencing the membership.
//     It mentions the user by ID: a room mention only pushes from power level 50, which ghosts lack.
//   - decline: org.matrix.msc4310.rtc.decline referencing the ring.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-meta/pkg/connector/callbridge"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

var (
	evtCallMember      = event.Type{Type: "org.matrix.msc3401.call.member", Class: event.StateEventType}
	evtRTCNotification = event.Type{Type: "org.matrix.msc4075.rtc.notification", Class: event.MessageEventType}
	evtRTCDecline      = event.Type{Type: "org.matrix.msc4310.rtc.decline", Class: event.MessageEventType}
)

const (
	// rtcDeviceID is the (virtual) device the ghosts join calls from.
	rtcDeviceID = "MESSENGERBRIDGE"
	// rtcMemberExpiry bounds a membership the bridge couldn't clear (a crash); it is renewed in long calls.
	rtcMemberExpiry = time.Hour
	// rtcRingLifetime is how long Element X rings (capped at 90 s by the clients).
	rtcRingLifetime = 90 * time.Second
)

// matrixRTCEnabled reports whether calls go through MatrixRTC instead of legacy m.call.*.
func (m *MetaClient) matrixRTCEnabled() bool {
	return m.Main.Config.CallBridging && m.Main.Config.CallBridgingMatrixRTC
}

// rtcFocus is the homeserver's LiveKit focus, discovered once (retried after failures).
type rtcFocus struct {
	lock  sync.Mutex
	focus *rtcTransport
	tried time.Time
}

func (m *MetaConnector) liveKitFocus(ctx context.Context) (*rtcTransport, error) {
	f := &m.rtcFocus
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.focus != nil {
		return f.focus, nil
	}
	if time.Since(f.tried) < time.Minute {
		return nil, errors.New("no LiveKit focus (discovery failed recently)")
	}
	f.tried = time.Now()
	bot, ok := m.Bridge.Bot.(*matrix.ASIntent)
	if !ok {
		return nil, errors.New("bot intent isn't an appservice intent")
	}
	focus, err := discoverRTCTransport(ctx, bot.Matrix.Client, m.Bridge.Matrix.ServerName())
	if err != nil {
		return nil, err
	}
	f.focus = focus
	return focus, nil
}

// rtcMemberContent is the membership Element Call sends in Compatibility mode (what Element X uses).
func rtcMemberContent(focus *rtcTransport, roomID id.RoomID, deviceID, userID string, video bool, createdTS int64) map[string]any {
	intent := "audio"
	if video {
		intent = "video"
	}
	content := map[string]any{
		"application":   "m.call",
		"call_id":       "",
		"scope":         "m.room",
		"device_id":     deviceID,
		"membershipID":  userID + ":" + deviceID,
		"expires":       rtcMemberExpiry.Milliseconds(),
		"m.call.intent": intent,
		"focus_active":  map[string]any{"type": "livekit", "focus_selection": "multi_sfu"},
		"foci_preferred": []any{map[string]any{
			"type":                "livekit",
			"livekit_service_url": focus.LivekitServiceURL,
			"livekit_alias":       roomID,
		}},
	}
	if createdTS != 0 {
		content["created_ts"] = createdTS
	}
	return content
}

// rtcStateKey is the per-device membership state key (rooms without MSC3757 owned state keep the "_").
func rtcStateKey(userID id.UserID, deviceID string) string {
	return "_" + string(userID) + "_" + deviceID + "_m.call"
}

// rtcMembership is what the bridge needs from someone's call.member event.
type rtcMembership struct {
	joined bool
	device string
	video  bool
}

func parseRTCMembership(evt *event.Event) rtcMembership {
	raw := evt.Content.Raw
	if len(raw) == 0 {
		return rtcMembership{}
	}
	app, _ := raw["application"].(string)
	scope, _ := raw["scope"].(string)
	device, _ := raw["device_id"].(string)
	intent, _ := raw["m.call.intent"].(string)
	if app != "m.call" || (scope != "" && scope != "m.room") {
		return rtcMembership{}
	}
	// An expired membership (a client that vanished) is not in the call.
	if exp, ok := raw["expires"].(float64); ok && exp > 0 {
		start := evt.Timestamp
		if created, ok := raw["created_ts"].(float64); ok && created > 0 {
			start = int64(created)
		}
		if time.Now().UnixMilli() > start+int64(exp) {
			return rtcMembership{}
		}
	}
	return rtcMembership{joined: true, device: device, video: intent == "video"}
}

// ensureCallPowerLevel lets the portal's members (the user and the ghosts) send call memberships:
// Element X only enables the call button, and a membership is only accepted, when the sender may send
// org.matrix.msc3401.call.member. Portals default state events to 50.
func (m *MetaConnector) ensureCallPowerLevel(ctx context.Context, roomID id.RoomID) error {
	bot, ok := m.Bridge.Bot.(*matrix.ASIntent)
	if !ok {
		return errors.New("bot intent isn't an appservice intent")
	}
	var pl event.PowerLevelsEventContent
	if err := bot.Matrix.StateEvent(ctx, roomID, event.StatePowerLevels, "", &pl); err != nil {
		return fmt.Errorf("get power levels: %w", err)
	}
	if pl.GetEventLevel(evtCallMember) <= pl.UsersDefault {
		return nil
	}
	if pl.Events == nil {
		pl.Events = map[string]int{}
	}
	pl.Events[evtCallMember.Type] = pl.UsersDefault
	if _, err := bot.Matrix.SendStateEvent(ctx, roomID, event.StatePowerLevels, "", &pl); err != nil {
		return fmt.Errorf("set power levels: %w", err)
	}
	return nil
}

// ensureDMCallPowerLevels prepares every DM portal of this login, so Element X offers the call button
// before any call happened.
func (m *MetaClient) ensureDMCallPowerLevels(ctx context.Context) {
	log := m.UserLogin.Log.With().Str("action", "ensure call power levels").Logger()
	portals, err := m.Main.Bridge.GetAllPortalsWithMXID(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to list portals")
		return
	}
	fixed := 0
	for _, portal := range portals {
		if portal.Receiver != m.UserLogin.ID || portal.RoomType != database.RoomTypeDM || ctx.Err() != nil {
			continue
		}
		if err := m.Main.ensureCallPowerLevel(ctx, portal.MXID); err != nil {
			log.Debug().Err(err).Stringer("room_id", portal.MXID).Msg("Failed to allow calls in portal")
			continue
		}
		fixed++
	}
	log.Info().Int("portals", fixed).Msg("DM portals allow MatrixRTC calls")
}

// ghostClient is the ghost's Matrix client (appservice-masqueraded).
func (s *callSession) ghostClient() (*mautrix.Client, error) {
	gi, ok := s.ghost.(*matrix.ASIntent)
	if !ok {
		return nil, errors.New("ghost intent isn't an appservice intent")
	}
	return gi.Matrix.Client, nil
}

// joinRTC joins the room's LiveKit call as the ghost.
func (s *callSession) joinRTC() error {
	focus, err := s.m.Main.liveKitFocus(s.ctx)
	if err != nil {
		return err
	}
	cli, err := s.ghostClient()
	if err != nil {
		return err
	}
	url, token, err := liveKitToken(s.ctx, cli, focus, s.portal.MXID, rtcDeviceID)
	if err != nil {
		return err
	}
	leg, err := callbridge.JoinRTC(s.ctx, callbridge.RTCLegConfig{URL: url, Token: token, Log: s.log})
	if err != nil {
		return err
	}
	s.lock.Lock()
	s.rtc = leg
	s.rtcFocus = focus
	s.lock.Unlock()
	return nil
}

// sendRTCMembership publishes (or renews) the ghost's call membership.
func (s *callSession) sendRTCMembership(ctx context.Context) error {
	cli, err := s.ghostClient()
	if err != nil {
		return err
	}
	s.lock.Lock()
	focus, created, video := s.rtcFocus, s.rtcCreatedTS, s.videoCodec != ""
	s.lock.Unlock()
	if focus == nil {
		return errors.New("no focus")
	}
	resp, err := cli.SendStateEvent(ctx, s.portal.MXID, evtCallMember, rtcStateKey(cli.UserID, rtcDeviceID),
		rtcMemberContent(focus, s.portal.MXID, rtcDeviceID, string(cli.UserID), video, created))
	if err != nil {
		return fmt.Errorf("send call membership: %w", err)
	}
	s.lock.Lock()
	if s.rtcCreatedTS == 0 {
		s.rtcCreatedTS = time.Now().UnixMilli()
	}
	s.rtcMemberEvent = resp.EventID
	s.rtcJoined = true
	s.lock.Unlock()
	// Renew before it expires, for calls longer than the expiry.
	time.AfterFunc(rtcMemberExpiry*3/4, func() {
		if s.ctx.Err() == nil {
			if err := s.sendRTCMembership(s.ctx); err != nil {
				s.log.Warn().Err(err).Msg("Failed to renew call membership")
			}
		}
	})
	return nil
}

// leaveRTC clears the ghost's membership and leaves LiveKit.
func (s *callSession) leaveRTC(ctx context.Context) {
	s.lock.Lock()
	joined, leg := s.rtcJoined, s.rtc
	s.rtcJoined = false
	s.lock.Unlock()
	if joined {
		if cli, err := s.ghostClient(); err == nil {
			_, err = cli.SendStateEvent(ctx, s.portal.MXID, evtCallMember, rtcStateKey(cli.UserID, rtcDeviceID), map[string]any{})
			if err != nil {
				s.log.Warn().Err(err).Msg("Failed to clear call membership")
			}
		}
	}
	if leg != nil {
		leg.Close()
	}
}

// ringRTC rings the user's Matrix clients for an incoming Messenger call: the ghost joins the call,
// then sends the ring. Joining (the user's membership or LiveKit presence) answers it.
func (s *callSession) ringRTC() error {
	if err := s.m.Main.ensureCallPowerLevel(s.ctx, s.portal.MXID); err != nil {
		s.log.Warn().Err(err).Msg("Couldn't allow call memberships in the portal")
	}
	if err := s.joinRTC(); err != nil {
		return fmt.Errorf("join MatrixRTC call: %w", err)
	}
	s.rtc.OnPeers(s.onRTCPeers)
	if err := s.sendRTCMembership(s.ctx); err != nil {
		return err
	}
	s.lock.Lock()
	memberEvent, video := s.rtcMemberEvent, s.videoCodec != ""
	s.lock.Unlock()
	intent := "audio"
	if video {
		intent = "video"
	}
	resp, err := s.ghost.SendMessage(s.ctx, s.portal.MXID, evtRTCNotification, &event.Content{Raw: map[string]any{
		"notification_type": "ring",
		"m.mentions":        map[string]any{"user_ids": []id.UserID{s.m.UserLogin.UserMXID}},
		"m.relates_to":      map[string]any{"rel_type": "m.reference", "event_id": memberEvent},
		"sender_ts":         time.Now().UnixMilli(),
		"lifetime":          rtcRingLifetime.Milliseconds(),
		"m.call.intent":     intent,
	}}, nil)
	if err != nil {
		return fmt.Errorf("send ring: %w", err)
	}
	s.lock.Lock()
	s.rtcRing = resp.EventID
	s.lock.Unlock()
	s.log.Info().Stringer("ring_event", resp.EventID).Msg("Rang Matrix (MatrixRTC)")
	return nil
}

// isUserIdentity reports whether a LiveKit identity ("@user:server:DEVICE") is the portal's user.
func (s *callSession) isUserIdentity(identity string) bool {
	return strings.HasPrefix(identity, string(s.m.UserLogin.UserMXID)+":")
}

// onRTCPeers answers an incoming call once the user shows up in LiveKit, and ends a call they left.
func (s *callSession) onRTCPeers(identities []string) {
	present := false
	for _, ident := range identities {
		if s.isUserIdentity(ident) {
			present = true
			break
		}
	}
	s.lock.Lock()
	wasPresent := s.rtcUserPresent
	s.rtcUserPresent = present
	s.lock.Unlock()
	switch {
	case present && !wasPresent:
		s.onRTCUserJoined()
	case !present && wasPresent:
		s.log.Info().Msg("The Matrix user left the MatrixRTC call")
		go s.end(endLocalHangup, "")
	}
}

// onRTCUserJoined answers an incoming Messenger call (once).
func (s *callSession) onRTCUserJoined() {
	if !s.incoming {
		return
	}
	s.lock.Lock()
	already := s.mxAnswered
	s.mxAnswered = true
	s.lock.Unlock()
	if already {
		return
	}
	s.log.Info().Msg("Answered in Matrix (MatrixRTC)")
	go s.answerMessenger()
}

// connectRTCOutgoing joins the call for an outgoing call once the Messenger peer answered.
func (s *callSession) connectRTCOutgoing() {
	s.lock.Lock()
	s.mxAnswered = true
	s.lock.Unlock()
	if err := s.joinRTC(); err != nil {
		s.log.Err(err).Msg("Failed to join the MatrixRTC call")
		s.end(endFailed, "Couldn't join the Element call")
		return
	}
	s.rtc.OnPeers(s.onRTCPeers)
	if err := s.sendRTCMembership(s.ctx); err != nil {
		s.log.Err(err).Msg("Failed to publish call membership")
		s.end(endFailed, "")
		return
	}
	s.startRelay()
}

// startRTCRelay forwards media between the Messenger leg and the LiveKit call.
func (s *callSession) startRTCRelay() {
	s.lock.Lock()
	metaLeg, rtc := s.metaLeg, s.rtc
	s.lock.Unlock()
	audio := func(name string, src func(context.Context) (*webrtc.TrackRemote, error), dst callbridge.RTPWriter) {
		tr, err := src(s.ctx)
		if err != nil {
			if s.ctx.Err() == nil {
				s.log.Warn().Err(err).Str("from", name).Msg("No remote audio")
			}
			return
		}
		var stats callbridge.RelayStats
		rlog := s.log.With().Str("from", name).Logger()
		err = callbridge.Relay(s.ctx, tr, uint8(tr.PayloadType()), dst, &stats, rlog)
		rlog.Info().AnErr("relay_err", err).Uint64("forwarded", stats.Forwarded.Load()).
			Uint64("dropped", stats.Dropped.Load()).Msg("Audio relay stopped")
	}
	go audio("messenger", metaLeg.RemoteTrack, rtc.AudioWriter())
	go audio("matrixrtc", rtc.RemoteAudio, metaLeg.Local)
	go s.relayMetaVideoToRTC(metaLeg, rtc)
	if metaLeg.LocalVideo != nil {
		go s.relayRTCVideoToMeta(rtc, metaLeg)
	}
	time.AfterFunc(callSetupTimeout, func() {
		if !s.mediaConnected.Load() && s.ctx.Err() == nil {
			s.log.Warn().Msg("Messenger media didn't connect in time")
			s.end(endFailed, "The call didn't connect")
		}
	})
}

// relayMetaVideoToRTC publishes Messenger's camera (whenever it starts) into the call.
func (s *callSession) relayMetaVideoToRTC(metaLeg *callbridge.Leg, rtc *callbridge.RTCLeg) {
	tr, err := metaLeg.RemoteVideoTrack(s.ctx)
	if err != nil {
		return
	}
	dst, err := rtc.AddVideoTrack(tr.Codec().MimeType)
	if err != nil {
		s.log.Warn().Err(err).Msg("Failed to publish Messenger video")
		return
	}
	ssrc := tr.SSRC()
	rtc.OnKeyframeRequest(func() { metaLeg.RequestKeyframe(ssrc) })
	metaLeg.RequestKeyframe(ssrc)
	var stats callbridge.RelayStats
	rlog := s.log.With().Str("from", "messenger").Str("codec", tr.Codec().MimeType).Logger()
	err = callbridge.RelayVideo(s.ctx, tr, uint8(tr.PayloadType()), dst, &stats, rlog)
	rlog.Info().AnErr("relay_err", err).Uint64("forwarded", stats.Forwarded.Load()).Msg("Video relay stopped")
}

// relayRTCVideoToMeta sends the Matrix user's camera to Messenger (video calls only).
func (s *callSession) relayRTCVideoToMeta(rtc *callbridge.RTCLeg, metaLeg *callbridge.Leg) {
	tr, err := rtc.RemoteVideo(s.ctx)
	if err != nil {
		return
	}
	if got, want := tr.Codec().MimeType, s.videoCodec; !strings.EqualFold(got, want) {
		s.log.Warn().Str("matrix_codec", got).Str("messenger_codec", want).
			Msg("The Element call's video codec differs from Messenger's, not relaying video")
		return
	}
	metaLeg.OnKeyframeRequest(func() { rtc.RequestKeyframe(tr) })
	rtc.RequestKeyframe(tr)
	var stats callbridge.RelayStats
	rlog := s.log.With().Str("from", "matrixrtc").Str("codec", tr.Codec().MimeType).Logger()
	err = callbridge.RelayVideo(s.ctx, tr, uint8(tr.PayloadType()), metaLeg.LocalVideo, &stats, rlog)
	rlog.Info().AnErr("relay_err", err).Uint64("forwarded", stats.Forwarded.Load()).Msg("Video relay stopped")
}

// startOutgoingRTC places a Messenger call for a MatrixRTC call the user started in a DM portal.
func (cb *callBridge) startOutgoingRTC(ctx context.Context, portal *bridgev2.Portal, video bool) {
	log := zerolog.Ctx(ctx)
	peerID := metaid.ParseFBPortalID(portal.ID)
	meta, _ := portal.Metadata.(*metaid.PortalMetadata)
	if peerID == 0 || meta == nil || peerID == cb.m.selfFBID() || portal.RoomType != database.RoomTypeDM {
		log.Debug().Msg("MatrixRTC call in a non-DM portal, not bridging")
		return
	}
	s, err := cb.newSession(ctx, portal, peerID, false, "", "")
	if err != nil {
		log.Info().Err(err).Msg("Not bridging MatrixRTC call")
		return
	}
	s.lock.Lock()
	s.rtcMode = true
	s.e2ee = meta.WhatsAppServer != ""
	if video {
		s.videoCodec = webrtc.MimeTypeVP8
	}
	s.lock.Unlock()
	s.log.Info().Bool("video", video).Msg("Outgoing MatrixRTC call from Matrix")
	id := s.m.callIdentity()
	if id == nil {
		s.end(endFailed, "The bridge has no Messenger encryption device, so it can't place calls")
		return
	}
	// The token and focus are ready by the time Messenger answers.
	go func() { _, _ = s.m.Main.liveKitFocus(s.ctx) }()
	err = s.placeMessengerCall(id, peerID)
	if errors.Is(err, errSFUPath) {
		s.log.Info().Msg("Messenger chose the SFU path, placing the call again")
		s.abandonMessengerAttempt()
		err = s.placeMessengerCall(id, peerID)
	}
	if err != nil {
		s.log.Err(err).Msg("Failed to start Messenger call")
		s.end(endFailed, "Failed to start the Messenger call")
		return
	}
	time.AfterFunc(callInviteLifetime, func() {
		s.lock.Lock()
		answered := s.metaAnswered
		s.lock.Unlock()
		if !answered && s.ctx.Err() == nil {
			s.log.Info().Msg("Messenger peer didn't answer")
			s.end(endTimeout, "")
		}
	})
}

// handleRTCMembership reacts to the user joining or leaving a MatrixRTC call in a portal.
func (cb *callBridge) handleRTCMembership(ctx context.Context, portal *bridgev2.Portal, evt *event.Event) {
	mem := parseRTCMembership(evt)
	cb.lock.Lock()
	s := cb.active
	cb.lock.Unlock()
	if s != nil && s.portal.MXID != portal.MXID {
		s = nil
	}
	switch {
	case mem.joined && s == nil:
		cb.startOutgoingRTC(ctx, portal, mem.video)
	case mem.joined && s != nil && s.rtcMode:
		s.onRTCUserJoined()
	case !mem.joined && s != nil && s.rtcMode:
		s.log.Info().Msg("The Matrix user left the call")
		go s.end(endLocalHangup, "")
	}
}

// handleRTCDecline ends a ringing incoming call the user declined.
func (cb *callBridge) handleRTCDecline(evt *event.Event) {
	rel, _ := evt.Content.Raw["m.relates_to"].(map[string]any)
	target, _ := rel["event_id"].(string)
	cb.lock.Lock()
	s := cb.active
	cb.lock.Unlock()
	if s == nil || !s.rtcMode {
		return
	}
	s.lock.Lock()
	ring, answered := s.rtcRing, s.mxAnswered
	s.lock.Unlock()
	if target != "" && id.EventID(target) == ring && !answered {
		s.log.Info().Msg("Declined in Matrix (MatrixRTC)")
		go s.end(endLocalDecline, "")
	}
}
