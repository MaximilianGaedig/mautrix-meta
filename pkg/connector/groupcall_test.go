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
	"context"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/callbridge"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
)

func TestGroupRingClassification(t *testing.T) {
	cc := func(json string) []rtcsignal.DataMessage {
		return []rtcsignal.DataMessage{{Topic: collisionContextTopic, Data: []byte(json)}}
	}
	oneToOne := &rtcsignal.RingRequest{
		Caller:            "1",
		OtherParticipants: []string{"2"},
		Offer:             &rtcsignal.SessionDescription{SDP: "v=0"},
		AppMessages:       cc(`{"group_thread_id":null,"peer_id":"1"}`),
	}
	if isGroupRing(oneToOne) {
		t.Error("a 1:1 ring with an offer was taken for a group call")
	}
	group := &rtcsignal.RingRequest{Caller: "1", AppMessages: cc(`{"group_thread_id":"555","peer_id":null}`)}
	if !isGroupRing(group) || groupThreadOf(group.AppMessages) != "555" {
		t.Error("a ring naming a group thread wasn't taken for a group call")
	}
	if !isGroupRing(&rtcsignal.RingRequest{OtherParticipants: []string{"2", "3"}}) {
		t.Error("a ring with several other participants wasn't taken for a group call")
	}
	// A 1:1 call that Messenger rings on its SFU: no offer, no group thread. It goes the group way,
	// and startIncomingGroup hands it to startIncomingSFU1to1.
	sfu1to1 := &rtcsignal.RingRequest{Caller: "1", OtherParticipants: []string{"2"}, MediaPath: rtcsignal.MediaPathSFU,
		AppMessages: cc(`{"group_thread_id":null,"peer_id":"1"}`)}
	if !isGroupRing(sfu1to1) || groupThreadOf(sfu1to1.AppMessages) != "" {
		t.Error("a 1:1 SFU ring wasn't routed to the SFU path")
	}
}

// TestNewGroupCallDoesNotDeadlock: newGroupCall used to call markBridged (which takes cb.lock) while
// holding cb.lock, wedging all call signalling on the first group ring.
func TestNewGroupCallDoesNotDeadlock(t *testing.T) {
	cb := &callBridge{log: zerolog.Nop(), m: &MetaClient{}}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "555"}}}
	done := make(chan error, 1)
	go func() {
		_, err := cb.newGroupCall(context.Background(), portal, "555", true)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("newGroupCall deadlocked")
	}
	if !cb.recentlyBridged(portal.PortalKey) {
		t.Error("the group call's portal isn't marked as bridged")
	}
	if _, err := cb.newGroupCall(context.Background(), portal, "555", true); err != errBusy {
		t.Errorf("second group call: %v, want errBusy", err)
	}
}

// A participant sharing their screen publishes it as a track of its own (label SCREEN) and pauses
// the camera; the bridge subscribed to cameras only, so the Matrix side saw a frozen camera and no
// screen. The screen track is subscribed to and known as a screen.
func TestGroupCallSubscribesToScreenShares(t *testing.T) {
	cb := &callBridge{log: zerolog.Nop(), m: &MetaClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "100"}}}}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "555"}}}
	g, err := cb.newGroupCall(context.Background(), portal, "555", true)
	if err != nil {
		t.Fatal(err)
	}
	status := map[string]rtcsignal.TrackInfo{
		"cam": {Enabled: true, Owner: "200", Label: rtcsignal.TrackLabelVideo},
		"scr": {Enabled: true, Owner: "200", Label: rtcsignal.TrackLabelScreen},
		"mic": {Enabled: true, Owner: "200", Label: rtcsignal.TrackLabelAudio},
	}
	g.handleServerMediaUpdate(&rtcsignal.Message{Body: rtcsignal.Body{ServerMediaUpdateRequest: &rtcsignal.ServerMediaUpdateRequest{MediaStatus: status}}})
	g.updateSubscriptions(status)
	g.lock.Lock()
	defer g.lock.Unlock()
	if !g.subscribed["scr"] || !g.subscribed["cam"] || g.subscribed["mic"] {
		t.Errorf("subscribed = %v, want the camera and the screen", g.subscribed)
	}
	if !g.screens["scr"] || g.screens["cam"] {
		t.Errorf("screens = %v, want only the screen track", g.screens)
	}
}

// The numbers a shared screen goes by on the wire are the web client's TrackLabel: 3 for its picture
// and 2 for its sound. The bridge had 2 for the picture: it took the sound of a shared tab for a
// screen to show, missed the screen itself, and labelled the Matrix user's screen as audio, which the
// web client never draws.
func TestScreenLabelsAreTheWebClients(t *testing.T) {
	const camera, screenAudio, screenVideo = 1, 2, 3
	status := map[string]rtcsignal.TrackInfo{
		"cam": {Enabled: true, Owner: "200", Label: camera},
		"scr": {Enabled: true, Owner: "200", Label: screenVideo},
		"tab": {Enabled: true, Owner: "200", Label: screenAudio},
	}
	smu := &rtcsignal.Message{Body: rtcsignal.Body{ServerMediaUpdateRequest: &rtcsignal.ServerMediaUpdateRequest{MediaStatus: status}}}

	// A 1:1 call.
	s := &callSession{cb: &callBridge{log: zerolog.Nop()}, log: zerolog.Nop()}
	s.handleServerMediaUpdate(smu)
	if !s.peerScreens["scr"] || s.peerScreens["tab"] || s.peerScreens["cam"] {
		t.Errorf("1:1 screens = %v, want only the screen's picture", s.peerScreens)
	}

	// A group call.
	cb := &callBridge{log: zerolog.Nop(), m: &MetaClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "100"}}}}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "555"}}}
	g, err := cb.newGroupCall(context.Background(), portal, "555", true)
	if err != nil {
		t.Fatal(err)
	}
	g.handleServerMediaUpdate(smu)
	g.updateSubscriptions(status)
	g.lock.Lock()
	if !g.subscribed["scr"] || !g.subscribed["cam"] || g.subscribed["tab"] {
		t.Errorf("group video subscriptions = %v, want the camera and the screen's picture", g.subscribed)
	}
	if !g.screens["scr"] || g.screens["tab"] || g.screens["cam"] {
		t.Errorf("group screens = %v, want only the screen's picture", g.screens)
	}
	g.lock.Unlock()

	// The Matrix user's own screen.
	leg := &callbridge.Leg{TrackID: "aud", ScreenTrackID: "own"}
	if leg.LocalScreen, err = webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264}, "own", "s"); err != nil {
		t.Fatal(err)
	}
	if got := metaTracks(leg, true, false, true)["own"].Label; got != screenVideo {
		t.Errorf("our screen in a 1:1 call is labelled %d, want %d", got, screenVideo)
	}
	g.setScreenOn(true)
	if got := g.ownTracks(leg, true, false)["own"].Label; got != screenVideo {
		t.Errorf("our screen in a group call is labelled %d, want %d", got, screenVideo)
	}
}

// Once the Matrix user's camera is added, the SFU is told about it with the audio, both owned by us:
// joined receive-only, the bridge never sent a camera into Messenger group calls.
func TestGroupCallReportsOwnCamera(t *testing.T) {
	cb := &callBridge{log: zerolog.Nop(), m: &MetaClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "100"}}}}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "556"}}}
	g, err := cb.newGroupCall(context.Background(), portal, "556", true)
	if err != nil {
		t.Fatal(err)
	}
	leg := &callbridge.Leg{TrackID: "aud", VideoTrackID: "vid"}
	if got := g.ownTracks(leg, true, true); len(got) != 1 || got["aud"].Label != rtcsignal.TrackLabelAudio {
		t.Fatalf("before the camera: %v, want only our audio", got)
	}
	leg.LocalVideo, err = webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "vid", "s")
	if err != nil {
		t.Fatal(err)
	}
	got := g.ownTracks(leg, true, false)
	if v := got["vid"]; v.Label != rtcsignal.TrackLabelVideo || v.Enabled || v.Owner != "100" {
		t.Errorf("our camera = %+v, want a disabled video track owned by us", v)
	}
	if a := got["aud"]; !a.Enabled || a.Owner != "100" {
		t.Errorf("our audio = %+v", a)
	}
}

// Element Call leaves a group call a while after the Messenger side does; a 1:1 call ringing in
// that gap was turned away as busy. A call everyone from Messenger has left is abandoned - one
// that is still ringing them is not.
func TestGroupCallAbandonedOnceEveryoneLeft(t *testing.T) {
	cb := &callBridge{log: zerolog.Nop(), m: &MetaClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "100"}}}}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "557"}}}
	g, err := cb.newGroupCall(context.Background(), portal, "557", false)
	if err != nil {
		t.Fatal(err)
	}
	state := func(s rtcsignal.ParticipantCallState) {
		g.handleConferenceState(&rtcsignal.ConferenceStateRequest{ParticipantStates: map[string]rtcsignal.ParticipantState{"200": {State: s}}})
	}
	state(rtcsignal.StateRinging)
	if g.abandoned() {
		t.Fatal("a call still ringing the others counted as abandoned")
	}
	state(rtcsignal.StateConnected)
	if g.abandoned() {
		t.Fatal("a call with someone in it counted as abandoned")
	}
	state(rtcsignal.StateDisconnected)
	if !g.abandoned() {
		t.Fatal("a call everyone left isn't abandoned")
	}
}

// A participant's media can start before the server says whose it is. The track was dropped as
// nobody's and never looked at again: a Messenger web participant stayed silent in the Matrix call.
func TestGroupCallTrackWaitsForItsOwner(t *testing.T) {
	cb := &callBridge{log: zerolog.Nop(), m: &MetaClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "100"}}}}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "555"}}}
	g, err := cb.newGroupCall(context.Background(), portal, "555", true)
	if err != nil {
		t.Fatal(err)
	}

	owner := make(chan string, 1)
	go func() { owner <- g.trackOwner("mic", "", 5*time.Second) }()
	select {
	case got := <-owner:
		t.Fatalf("a track nobody has named yet was given to %q", got)
	case <-time.After(50 * time.Millisecond):
	}

	status := map[string]rtcsignal.TrackInfo{"mic": {Enabled: true, Owner: "200", Label: rtcsignal.TrackLabelAudio}}
	g.handleServerMediaUpdate(&rtcsignal.Message{Body: rtcsignal.Body{ServerMediaUpdateRequest: &rtcsignal.ServerMediaUpdateRequest{MediaStatus: status}}})
	select {
	case got := <-owner:
		if got != "200" {
			t.Errorf("owner = %q, want the participant the media status named", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the track was not given to its owner once they were named")
	}

	// A track nobody ever names is given up on, and one named by its stream needs no waiting.
	if got := g.trackOwner("ghost", "", 20*time.Millisecond); got != "" {
		t.Errorf("an unnamed track got owner %q", got)
	}
	if got := g.trackOwner("other", "300:audio", time.Second); got != "300" {
		t.Errorf("owner from the stream id = %q", got)
	}
}

// A screen shared in Element went nowhere: the bridge had a camera track towards Messenger and no
// other. It is a track of its own, labelled as a screen, and off when nothing is being shared - so
// Messenger shows the share and takes it down again instead of keeping its last frame.
func TestSharedScreenIsATrackOfItsOwn(t *testing.T) {
	cb := &callBridge{log: zerolog.Nop(), m: &MetaClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "100"}}}}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "557"}}}
	g, err := cb.newGroupCall(context.Background(), portal, "557", true)
	if err != nil {
		t.Fatal(err)
	}
	leg := &callbridge.Leg{TrackID: "aud", VideoTrackID: "vid", ScreenTrackID: "scr"}
	if got := metaTracks(leg, true, true, true); len(got) != 1 {
		t.Fatalf("before anything is shared: %v, want only our audio", got)
	}
	leg.LocalScreen, err = webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264}, "scr", "s")
	if err != nil {
		t.Fatal(err)
	}

	// A 1:1 call: the screen beside the microphone, with no camera at all.
	one := metaTracks(leg, true, false, true)
	if s := one["scr"]; s.Label != rtcsignal.TrackLabelScreen || !s.Enabled {
		t.Errorf("1:1 screen = %+v, want an enabled screen track", s)
	}
	if _, camera := one["vid"]; camera {
		t.Errorf("a camera was reported that isn't there: %v", one)
	}
	if s := metaTracks(leg, true, false, false)["scr"]; s.Enabled {
		t.Errorf("the screen stays on after the share ended: %+v", s)
	}

	// A group call: the same, owned by us.
	g.setScreenOn(true)
	if s := g.ownTracks(leg, true, false)["scr"]; s.Label != rtcsignal.TrackLabelScreen || !s.Enabled || s.Owner != "100" {
		t.Errorf("group screen = %+v, want an enabled screen track owned by us", s)
	}
	g.setScreenOn(false)
	if s := g.ownTracks(leg, true, false)["scr"]; s.Enabled {
		t.Errorf("the group screen stays on after the share ended: %+v", s)
	}
}

// Messenger's web client offers VP8 and H264, its phone apps only H264, and Element Call sends H264.
// Taking VP8 because it was offered left the two legs on different codecs: no video from Element.
func TestRTCVideoCodecFollowsElementCall(t *testing.T) {
	web := "v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96 108\r\na=rtpmap:96 VP8/90000\r\na=rtpmap:108 H264/90000\r\na=sendrecv\r\n"
	if got := rtcVideoCodec(web); got != webrtc.MimeTypeH264 {
		t.Errorf("web offer: %q, want H264", got)
	}
	phone := "v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 108\r\na=rtpmap:108 H264/90000\r\na=sendrecv\r\n"
	if got := rtcVideoCodec(phone); got != webrtc.MimeTypeH264 {
		t.Errorf("phone offer: %q, want H264", got)
	}
	old := "v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=rtpmap:96 VP8/90000\r\na=sendrecv\r\n"
	if got := rtcVideoCodec(old); got != webrtc.MimeTypeVP8 {
		t.Errorf("VP8-only offer: %q, want VP8 rather than no video", got)
	}
}

// A Messenger peer that offers only VP8 gets a VP8 leg, and Element Call sends H264: the bridge has
// to ask LiveKit for the VP8 backup, or its own camera relay refuses every track as the wrong codec.
func TestRTCJoinCodec(t *testing.T) {
	if got := rtcJoinCodec(webrtc.MimeTypeVP8); got != webrtc.MimeTypeVP8 {
		t.Errorf("VP8 leg joins asking for %q, want VP8", got)
	}
	// H264 is what arrives anyway; restricting would only lose retransmissions. No video: nothing to ask.
	for _, leg := range []string{webrtc.MimeTypeH264, ""} {
		if got := rtcJoinCodec(leg); got != "" {
			t.Errorf("leg %q joins asking for %q, want no restriction", leg, got)
		}
	}
}
