package metacall

import (
	"slices"
	"strings"
	"testing"

	"github.com/pion/webrtc/v4"
	"github.com/rs/zerolog"
	"go.mau.fi/libsignal/ecc"
	"maunium.net/go/mautrix/bridgev2/callbridge"

	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
)

func TestPrepareLocalSDP(t *testing.T) {
	kp, err := ecc.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	id := &Identity{
		UserID: 100000000000002, DeviceID: 7,
		Priv: kp.PrivateKey().Serialize(), Pub: kp.PublicKey().PublicKey(),
	}
	sdp := "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=group:BUNDLE 0 1\r\n" +
		"m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=ice-ufrag:u\r\na=ice-pwd:p\r\n" +
		"a=fingerprint:sha-256 ab:cd\r\na=sendrecv\r\n" +
		"m=video 9 UDP/TLS/RTP/SAVPF 96\r\na=sendonly\r\n"
	prepared, err := PrepareLocalSDP(sdp, id, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"a=fingerprint:sha-256 AB:CD", webICEOptions, "m=video 9 UDP/TLS/RTP/SAVPF 96\r\na=inactive"} {
		if !strings.Contains(prepared, want) {
			t.Errorf("prepared SDP lacks %q", want)
		}
	}
	info, err := rtcsignal.VerifyDTLSAuth(prepared, id.UserID, append([]byte{ecc.DjbType}, id.Pub[:]...))
	if err != nil {
		t.Fatalf("x-dtls-auth does not verify: %v", err)
	}
	if info.DeviceID != id.DeviceID {
		t.Errorf("device id %d", info.DeviceID)
	}
	if got := PrepareRemoteSDP(prepared); strings.Contains(got, "x-dtls-auth") {
		t.Error("Meta DTLS auth leaked into Pion SDP")
	}
}

// Messenger's peers act as the controlled ICE agent, so the bridge must control: marking their SDP
// ICE-lite makes Pion take that role on every leg, incoming calls included.
func TestPrepareRemoteSDPMakesTheBridgeControl(t *testing.T) {
	in := "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\n"
	out := PrepareRemoteSDP(in)
	want := "t=0 0\r\na=ice-lite\r\nm=audio"
	if !strings.Contains(out, want) {
		t.Fatalf("no session-level a=ice-lite before the first m= line:\n%q", out)
	}
	if again := PrepareRemoteSDP(out); strings.Count(again, "a=ice-lite") != 1 {
		t.Fatalf("marked twice:\n%q", again)
	}
}

// phoneOffer is an offer shaped like the one Messenger's iPhone app rings with: Plan B, H264 under
// two payload types, FlexFEC and Meta's own FEC beside them, one track with its FEC stream, and a
// data channel. The two H264 lines are libwebrtc's on iOS (constrained high, then constrained
// baseline); the real ones were never logged.
const phoneOffer = "v=0\r\no=- 4611731400430051336 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n" +
	"a=group:BUNDLE audio video data\r\na=msid-semantic: WMS stream\r\n" +
	"m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n" + phoneTransport + "a=mid:audio\r\na=sendrecv\r\na=rtcp-mux\r\n" +
	"a=rtpmap:111 opus/48000/2\r\na=fmtp:111 minptime=10;useinbandfec=1\r\n" +
	"a=ssrc:1001 cname:c\r\na=ssrc:1001 msid:stream audio0\r\n" +
	"m=video 9 UDP/TLS/RTP/SAVPF 98 99 103 104\r\n" + phoneTransport + "a=mid:video\r\n" +
	"a=extmap:6 http://www.webrtc.org/experiments/rtp-hdrext/video-content-type\r\n" +
	"a=extmap:12 http://www.facebook.com/experiments/rtp-hdrext/video-frame-info\r\n" +
	"a=sendrecv\r\na=rtcp-mux\r\na=rtcp-rsize\r\n" +
	"a=rtpmap:98 H264/90000\r\na=rtcp-fb:98 nack pli\r\na=fmtp:98 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=640c1f\r\n" +
	"a=rtpmap:99 H264/90000\r\na=rtcp-fb:99 nack pli\r\na=fmtp:99 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f\r\n" +
	"a=rtpmap:103 flexfec-03/90000\r\na=fmtp:103 repair-window=10000000\r\na=rtpmap:104 rscode-02/90000\r\n" +
	"a=ssrc-group:FEC-FR 2001 2002\r\na=ssrc:2001 cname:c\r\na=ssrc:2001 msid:stream video0\r\n" +
	"a=ssrc:2002 cname:c\r\na=ssrc:2002 msid:stream video0\r\n" +
	"m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n" + phoneTransport + "a=mid:data\r\na=sctp-port:5000\r\n"

const phoneTransport = "c=IN IP4 0.0.0.0\r\na=ice-ufrag:abcd\r\na=ice-pwd:abcdefghijklmnopqrstuvwx\r\n" +
	"a=fingerprint:sha-256 00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF\r\n" +
	"a=setup:actpass\r\n"

func TestVideoFormats(t *testing.T) {
	want := []string{
		"98 H264/90000 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=640c1f",
		"99 H264/90000 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
		"103 flexfec-03/90000 repair-window=10000000",
		"104 rscode-02/90000",
	}
	if got := VideoFormats(phoneOffer); !slices.Equal(got, want) {
		t.Errorf("video formats = %q, want %q", got, want)
	}
	if got := VideoFormats("v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=rtpmap:111 opus/48000/2\r\n"); len(got) != 0 {
		t.Errorf("an audio call has video formats: %q", got)
	}
}

// What the Messenger leg lets a phone send. It registers one H264 (constrained baseline,
// packetization mode 1), and Pion answers with the offered payload types that match it exactly when
// any does: of a phone's two H264s only the baseline one is accepted. That is the payload type the
// phone's camera was seen on, and it leaves the phone no other to put anything under - a screen in
// the camera's track could not have come under the other one. This records what the leg does today,
// to be read with the "Video formats agreed with Messenger" log line of a real call.
func TestPhoneOfferKeepsOneH264(t *testing.T) {
	se := &webrtc.SettingEngine{}
	se.SetIncludeLoopbackCandidate(true)
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	se.SetInterfaceFilter(func(name string) bool { return name == "lo" || name == "lo0" })
	leg, err := callbridge.NewLeg(callbridge.LegConfig{
		Name: "messenger", WebShape: true, VideoCodec: webrtc.MimeTypeH264, PlanB: callbridge.IsPlanB(phoneOffer),
		Settings: se, Log: zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer leg.Close()
	if !leg.IsPlanB() {
		t.Fatal("the phone's offer wasn't taken for Plan B")
	}
	answer, err := leg.AnswerOffer(PrepareRemoteSDP(phoneOffer))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"99 H264/90000 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"}
	if got := VideoFormats(answer); !slices.Equal(got, want) {
		t.Errorf("accepted video formats = %q, want %q", got, want)
	}
}
