package metacall

import (
	"strings"
	"testing"

	"go.mau.fi/libsignal/ecc"

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
