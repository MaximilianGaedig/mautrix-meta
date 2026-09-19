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

package callsignal

import (
	"bytes"
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/rs/zerolog"

	"go.mau.fi/mautrix-meta/pkg/messagix/dgw"
	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
)

const (
	testSelf     = "100000000000002"
	testCaller   = "100000000000001"
	testRoom     = "ROOM:1000000000000009"
	testDeviceID = "00000000-0000-4000-8000-00000000d001"
)

// fakeDGW is a minimal rpsignaling server: it answers EstablishStream,
// acks every data frame, and exposes the multiway messages the client sent.
type fakeDGW struct {
	t        *testing.T
	conn     *websocket.Conn
	query    chan string
	received chan *rtcsignal.Message
	acks     chan uint16
	nextAck  uint16
}

func newFakeDGW(t *testing.T) (*fakeDGW, *httptest.Server) {
	f := &fakeDGW{t: t, query: make(chan string, 1), received: make(chan *rtcsignal.Message, 16), acks: make(chan uint16, 16)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.query <- r.URL.RawQuery
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		f.conn = conn
		ctx := r.Context()
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			for len(data) > 0 {
				frame := dgw.CheckFrameType(data)
				if data, err = frame.Unmarshal(data); err != nil {
					t.Errorf("bad frame from client: %v", err)
					return
				}
				switch fr := frame.(type) {
				case *dgw.EstablishStreamFrame:
					if string(fr.RawParameters) != "{}" || fr.StreamID != 0 {
						t.Errorf("unexpected establish: %s", fr)
					}
					f.write(&dgw.EstablishStreamFrame{StreamID: 0, RawParameters: []byte(`{"code":200}`)})
				case *dgw.DataFrame:
					if !fr.RequiresAck {
						t.Errorf("client data frame without RequiresAck")
					}
					f.write(&dgw.AckFrame{StreamID: fr.StreamID, AckID: fr.AckID})
					msg, err := rtcsignal.DecodePayload(fr.Payload, false)
					if err != nil {
						t.Errorf("client payload: %v", err)
						continue
					}
					f.received <- msg
				case *dgw.AckFrame:
					f.acks <- fr.AckID
				case *dgw.PingFrame:
					f.write(&dgw.PongFrame{})
				}
			}
		}
	}))
	return f, srv
}

func (f *fakeDGW) write(frame dgw.Frame) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.conn.Write(ctx, websocket.MessageBinary, frame.MarshalAppend(nil)); err != nil {
		f.t.Errorf("server write: %v", err)
	}
}

// push sends a server message as an acked data frame.
func (f *fakeDGW) push(msg *rtcsignal.Message) uint16 {
	b, err := msg.Marshal()
	if err != nil {
		f.t.Fatal(err)
	}
	env := (&rtcsignal.Envelope{Payload: b, ServiceType: rtcsignal.ServiceMWS}).MarshalResponse()
	id := f.nextAck
	f.nextAck++
	f.write(&dgw.DataFrame{StreamID: 0, Payload: env, RequiresAck: true, AckID: id})
	return id
}

func (f *fakeDGW) next(t *testing.T) *rtcsignal.Message {
	t.Helper()
	select {
	case msg := <-f.received:
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a client message")
		return nil
	}
}

// TestClientRingAndJoin runs the client against a fake rpsignaling server:
// connection parameters, stream establishment, acking a pushed RING,
// the default RingResponse, and a JOIN request/response round trip.
func TestClientRingAndJoin(t *testing.T) {
	f, srv := newFakeDGW(t)
	defer srv.Close()

	rings := make(chan *rtcsignal.Message, 1)
	c := New(Options{
		GetCookies:     func() string { return "" },
		RPSignalingURL: srv.URL + rtcsignal.RPSignalingPath,
		AppID:          "2220391788200892",
		UserID:         testSelf,
		DeviceID:       testDeviceID,
		Log:            zerolog.Nop(),
		Handler: func(ctx context.Context, msg *rtcsignal.Message, via Transport) *rtcsignal.Message {
			if msg.Body.RingRequest != nil && via == TransportDGW {
				rings <- msg
			}
			return nil
		},
	})
	c.Start(context.Background())
	defer c.Stop()

	q := <-f.query
	for _, want := range []string{"x-dgw-app-useUnifiedStream=true", "x-dgw-deviceid=" + testDeviceID, "x-dgw-app-stream-group=group1", "x-dgw-uuid=" + testSelf} {
		if !bytes.Contains([]byte(q), []byte(want)) {
			t.Errorf("connection query lacks %s", want)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.WaitConnected(ctx); err != nil {
		t.Fatal(err)
	}

	ring := &rtcsignal.Message{
		Header: rtcsignal.Header{Type: rtcsignal.TypeRing, ConferenceName: testRoom, TransactionID: "17000000000000000001",
			ServerInfoData: "c2lk", SequenceNumber: 1, ConferenceType: rtcsignal.ConferenceTypeRoom, ReceiverUserID: testSelf},
		Body: rtcsignal.Body{RingRequest: &rtcsignal.RingRequest{Caller: testCaller, OtherParticipants: []string{testSelf},
			Offer: &rtcsignal.SessionDescription{SDP: "v=0\r\n"}, MediaPath: rtcsignal.MediaPathP2P}},
	}
	ackID := f.push(ring)
	select {
	case got := <-rings:
		if got.Body.RingRequest.Caller != testCaller {
			t.Error("wrong ring delivered")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler didn't get the RING")
	}
	select {
	case id := <-f.acks:
		if id != ackID {
			t.Errorf("acked %d, want %d", id, ackID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RING data frame wasn't acked")
	}
	resp := f.next(t)
	if resp.Header.Type != rtcsignal.TypeRing || resp.Header.TransactionID != ring.Header.TransactionID ||
		resp.Header.ResponseStatusCode != rtcsignal.StatusOK || resp.Body.RingResponse == nil || resp.Header.Has(20) {
		t.Errorf("unexpected ring response: %+v", resp.Header)
	}

	// JOIN round trip, with the response hook running before Request returns.
	cc := rtcsignal.NewCallContext(testSelf, testRoom, "c2lk")
	join := cc.NewJoin(&rtcsignal.JoinParams{Answer: "v=0\r\n", PeerID: testCaller})
	done := make(chan *rtcsignal.Message, 1)
	hooked := make(chan bool, 1)
	go func() {
		r, err := c.RequestHook(ctx, join, func(*rtcsignal.Message) { hooked <- true })
		if err != nil {
			t.Error(err)
		}
		done <- r
	}()
	sent := f.next(t)
	if sent.Body.JoinRequest == nil || sent.Body.JoinRequest.Answer == nil {
		t.Fatal("server didn't get the JOIN")
	}
	f.push(&rtcsignal.Message{
		Header: rtcsignal.Header{Type: rtcsignal.TypeJoin, ConferenceName: testRoom, TransactionID: sent.Header.TransactionID,
			ResponseStatusCode: rtcsignal.StatusOK, ResponseSubCode: rtcsignal.SubCodeOK},
		Body: rtcsignal.Body{JoinResponse: &rtcsignal.JoinResponse{MediaPath: rtcsignal.MediaPathP2P}},
	})
	select {
	case r := <-done:
		if r == nil || r.Body.JoinResponse == nil || r.Body.JoinResponse.MediaPath != rtcsignal.MediaPathP2P {
			t.Errorf("bad JOIN response")
		}
		if len(hooked) != 1 {
			t.Error("response hook didn't run before Request returned")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("JOIN request didn't complete")
	}
}

// TestMQTTPackets checks the MQTT packets against the captured bytes.
func TestMQTTPackets(t *testing.T) {
	// SUBSCRIBE id 1 to /t_rtc_multi at QoS 0, byte for byte as captured.
	want, _ := hex.DecodeString("82110001000c2f745f7274635f6d756c746900")
	if got := encodeMQTTSubscribe(1, rtcsignal.MQTTTopic); !bytes.Equal(got, want) {
		t.Errorf("SUBSCRIBE %x, want %x", got, want)
	}
	// CONNECT prefix: MQIsdp level 3, flags 0x82, keepalive 15, "mqttwsclient".
	conn := encodeMQTTConnect([]byte(`{"u":"1"}`))
	typ, _, body, err := parseMQTTPacket(conn)
	if err != nil || typ != mqttConnect {
		t.Fatalf("CONNECT parse: %v", err)
	}
	wantPrefix, _ := hex.DecodeString("00064d5149736470038200" + "0f000c6d7174747773636c69656e74")
	if !bytes.HasPrefix(body, wantPrefix) {
		t.Errorf("CONNECT variable header %x", body[:len(wantPrefix)])
	}
	// A QoS 1 retained PUBLISH like the captured RING delivery.
	payload := []byte{0x00, 0x01, 0x02}
	pkt := mqttPacket(mqttPublish<<4|0x03, append(append(appendMQTTString(nil, []byte(rtcsignal.MQTTTopic)), 0x12, 0x2e), payload...))
	typ, flags, body, err := parseMQTTPacket(pkt)
	if err != nil || typ != mqttPublish {
		t.Fatalf("PUBLISH parse: %v", err)
	}
	pub, err := parseMQTTPublish(flags, body)
	if err != nil || pub.Topic != rtcsignal.MQTTTopic || pub.QoS != 1 || pub.PacketID != 0x122e || !bytes.Equal(pub.Payload, payload) {
		t.Errorf("PUBLISH: %+v %v", pub, err)
	}
	// Long remaining length (the captured RING was 4340 bytes).
	big := mqttPacket(mqttPublish<<4, append(appendMQTTString(nil, []byte("t")), make([]byte, 4321)...))
	if _, _, body, err = parseMQTTPacket(big); err != nil || len(body) != 4324 {
		t.Errorf("long packet: %d %v", len(body), err)
	}
}
