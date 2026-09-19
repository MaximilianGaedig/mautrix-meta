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
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/rs/zerolog"

	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
)

// The parent facebook.com window's /t_rtc_multi connection, as captured:
//
//	wss://edge-chat.facebook.com/chat?region=<r>&sid=<s>&cid=<device id>
//	CONNECT  MQIsdp level 3, flags 0x82 (username, clean session),
//	         keepalive 15 s, client id "mqttwsclient", username = JSON below
//	SUBSCRIBE id 1: /t_rtc_multi QoS 0      -> SUBACK
//	PUBLISH  QoS 1 /t_rtc_multi (RING)      -> PUBACK
//	PINGREQ every 15 s                      -> PINGRESP
//
// The connection publishes nothing; replies go out on rpsignaling.

const (
	mqttKeepAlive      = 15 * time.Second
	mqttPongTimeout    = 30 * time.Second
	mqttConnectTimeout = 20 * time.Second
	mqttClientID       = "mqttwsclient"
)

const (
	mqttConnect   = 1
	mqttConnAck   = 2
	mqttPublish   = 3
	mqttPubAck    = 4
	mqttSubscribe = 8
	mqttSubAck    = 9
	mqttPingReq   = 12
	mqttPingResp  = 13
)

// mqttConnectPayload is the CONNECT username JSON, in the web client's key
// order.
type mqttConnectPayload struct {
	UserAgent   string `json:"a"`
	Asi         any    `json:"asi"`
	AppID       int64  `json:"aid"`
	Aids        any    `json:"aids"`
	ChatOn      bool   `json:"chat_on"`
	Cp          int    `json:"cp"`
	ClientType  string `json:"ct"`
	DeviceID    string `json:"d"`
	Dc          string `json:"dc"`
	Ecp         int    `json:"ecp"`
	Foreground  bool   `json:"fg"`
	Gas         any    `json:"gas"`
	MQTTSid     string `json:"mqtt_sid"`
	NoAutoFg    bool   `json:"no_auto_fg"`
	P           any    `json:"p"`
	Pack        []any  `json:"pack"`
	PHPOverride string `json:"php_override"`
	Pm          []any  `json:"pm"`
	SessionID   int64  `json:"s"`
	St          []any  `json:"st"`
	UserID      string `json:"u"`
}

type mqttListener struct {
	client    *Client
	opts      Options
	sessionID int64
	connLock  sync.Mutex
	conn      *websocket.Conn
}

func randomSessionID() int64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return int64(binary.BigEndian.Uint64(b[:]) & (1<<53 - 1))
}

func newMQTTListener(c *Client, opts Options) *mqttListener {
	return &mqttListener{client: c, opts: opts, sessionID: randomSessionID()}
}

func (m *mqttListener) close() {
	m.connLock.Lock()
	conn := m.conn
	m.connLock.Unlock()
	if conn != nil {
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}
}

func (m *mqttListener) loop(ctx context.Context) {
	log := zerolog.Ctx(ctx).With().Str("socket", "rtc_mqtt").Logger()
	ctx = log.WithContext(ctx)
	failures := 0
	for {
		start := time.Now()
		err := m.run(ctx)
		if ctx.Err() != nil {
			return
		}
		if time.Since(start) > 5*time.Minute {
			failures = 0
		}
		failures++
		backoff := min(time.Duration(1<<min(failures, 9))*time.Second, maxBackoff)
		log.Warn().Err(err).Int("failures", failures).Dur("reconnect_in", backoff).
			Msg("/t_rtc_multi MQTT connection lost, reconnecting")
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

func (m *mqttListener) url() string {
	q := url.Values{}
	if m.opts.MQTTRegion != "" {
		q.Set("region", m.opts.MQTTRegion)
	}
	q.Set("sid", strconv.FormatInt(m.sessionID, 10))
	q.Set("cid", m.opts.DeviceID)
	return m.opts.MQTTURL + "?" + q.Encode()
}

// EncodeMQTTConnect builds the CONNECT packet of the web client.
func encodeMQTTConnect(username []byte) []byte {
	var vh []byte
	vh = appendMQTTString(vh, []byte("MQIsdp"))
	vh = append(vh, 3, 0x82, byte(mqttKeepAlive/time.Second>>8), byte(mqttKeepAlive/time.Second))
	vh = appendMQTTString(vh, []byte(mqttClientID))
	vh = appendMQTTString(vh, username)
	return mqttPacket(mqttConnect<<4, vh)
}

func encodeMQTTSubscribe(packetID uint16, topic string) []byte {
	body := []byte{byte(packetID >> 8), byte(packetID)}
	body = appendMQTTString(body, []byte(topic))
	body = append(body, 0) // QoS 0, as the web client requests
	return mqttPacket(mqttSubscribe<<4|0x02, body)
}

func appendMQTTString(b, s []byte) []byte {
	b = append(b, byte(len(s)>>8), byte(len(s)))
	return append(b, s...)
}

func mqttPacket(first byte, body []byte) []byte {
	out := []byte{first}
	n := len(body)
	for {
		d := byte(n % 128)
		n /= 128
		if n > 0 {
			d |= 0x80
		}
		out = append(out, d)
		if n == 0 {
			break
		}
	}
	return append(out, body...)
}

// mqttPublishInfo is a parsed PUBLISH.
type mqttPublishInfo struct {
	Topic    string
	QoS      byte
	PacketID uint16
	Payload  []byte
}

var errMQTTShort = errors.New("callsignal: truncated MQTT packet")

// parseMQTTPacket splits one MQTT packet (the web socket carries one packet
// per message) into its type, flags and body.
func parseMQTTPacket(b []byte) (typ, flags byte, body []byte, err error) {
	if len(b) < 2 {
		return 0, 0, nil, errMQTTShort
	}
	p, length, mul := 1, 0, 1
	for {
		if p >= len(b) || p > 4 {
			return 0, 0, nil, errMQTTShort
		}
		c := b[p]
		p++
		length += int(c&0x7f) * mul
		mul *= 128
		if c < 0x80 {
			break
		}
	}
	if p+length > len(b) {
		return 0, 0, nil, errMQTTShort
	}
	return b[0] >> 4, b[0] & 0x0f, b[p : p+length], nil
}

func parseMQTTPublish(flags byte, body []byte) (*mqttPublishInfo, error) {
	if len(body) < 2 {
		return nil, errMQTTShort
	}
	tl := int(body[0])<<8 | int(body[1])
	if 2+tl > len(body) {
		return nil, errMQTTShort
	}
	pub := &mqttPublishInfo{Topic: string(body[2 : 2+tl]), QoS: (flags >> 1) & 3}
	rest := body[2+tl:]
	if pub.QoS > 0 {
		if len(rest) < 2 {
			return nil, errMQTTShort
		}
		pub.PacketID = uint16(rest[0])<<8 | uint16(rest[1])
		rest = rest[2:]
	}
	pub.Payload = rest
	return pub, nil
}

func (m *mqttListener) run(ctx context.Context) error {
	log := zerolog.Ctx(ctx)
	dialOpts := m.opts.DialOpts
	dialOpts.HTTPHeader = http.Header{}
	dialOpts.HTTPHeader.Set("cookie", m.opts.GetCookies())
	if m.opts.UserAgent != "" {
		dialOpts.HTTPHeader.Set("user-agent", m.opts.UserAgent)
	}
	dialOpts.HTTPHeader.Set("origin", m.opts.Origin)
	dialCtx, cancelDial := context.WithTimeout(ctx, mqttConnectTimeout)
	conn, _, err := websocket.Dial(dialCtx, m.url(), &dialOpts)
	cancelDial()
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	conn.SetReadLimit(1 << 22)
	m.connLock.Lock()
	m.conn = conn
	m.connLock.Unlock()
	defer func() {
		m.connLock.Lock()
		m.conn = nil
		m.connLock.Unlock()
		_ = conn.CloseNow()
	}()

	appID, _ := strconv.ParseInt(m.opts.AppID, 10, 64)
	username, _ := json.Marshal(&mqttConnectPayload{
		UserAgent: m.opts.UserAgent, AppID: appID, Cp: 3, ClientType: "websocket",
		DeviceID: m.opts.DeviceID, Ecp: 10, NoAutoFg: true, SessionID: m.sessionID,
		UserID: m.opts.UserID, Pack: []any{}, Pm: []any{}, St: []any{},
	})
	write := func(pkt []byte) error {
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return conn.Write(wctx, websocket.MessageBinary, pkt)
	}
	if err = write(encodeMQTTConnect(username)); err != nil {
		return fmt.Errorf("write CONNECT: %w", err)
	}

	lastPong := time.Now()
	var lastPongLock sync.Mutex
	pingCtx, stopPing := context.WithCancel(ctx)
	defer stopPing()
	subscribed := false
	for {
		msgType, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if msgType != websocket.MessageBinary {
			continue
		}
		typ, flags, body, err := parseMQTTPacket(data)
		if err != nil {
			return err
		}
		switch typ {
		case mqttConnAck:
			if len(body) < 2 || body[1] != 0 {
				return fmt.Errorf("CONNACK refused (code %v)", body)
			}
			if err = write(encodeMQTTSubscribe(1, rtcsignal.MQTTTopic)); err != nil {
				return fmt.Errorf("write SUBSCRIBE: %w", err)
			}
			go func() {
				ticker := time.NewTicker(mqttKeepAlive)
				defer ticker.Stop()
				for {
					select {
					case <-pingCtx.Done():
						return
					case <-ticker.C:
						lastPongLock.Lock()
						stale := time.Since(lastPong) > mqttPongTimeout
						lastPongLock.Unlock()
						if stale {
							log.Warn().Msg("/t_rtc_multi MQTT ping timeout")
							_ = conn.CloseNow()
							return
						}
						if err := write([]byte{mqttPingReq << 4, 0}); err != nil {
							_ = conn.CloseNow()
							return
						}
					}
				}
			}()
		case mqttSubAck:
			if !subscribed {
				subscribed = true
				log.Info().Msg("Subscribed to /t_rtc_multi")
			}
		case mqttPingResp:
			lastPongLock.Lock()
			lastPong = time.Now()
			lastPongLock.Unlock()
		case mqttPublish:
			pub, err := parseMQTTPublish(flags, body)
			if err != nil {
				return err
			}
			if pub.QoS > 0 {
				if err = write([]byte{mqttPubAck << 4, 2, byte(pub.PacketID >> 8), byte(pub.PacketID)}); err != nil {
					return fmt.Errorf("write PUBACK: %w", err)
				}
			}
			if pub.Topic != rtcsignal.MQTTTopic {
				log.Debug().Str("topic", pub.Topic).Msg("Ignoring MQTT publish on unexpected topic")
				continue
			}
			_, msg, err := rtcsignal.DecodeMQTTPayload(pub.Payload)
			if err != nil {
				log.Warn().Err(err).Int("len", len(pub.Payload)).Msg("Failed to decode /t_rtc_multi payload")
				continue
			}
			m.client.dispatch(ctx, msg, TransportMQTT)
		default:
			log.Debug().Uint8("mqtt_type", typ).Msg("Unhandled MQTT packet")
		}
	}
}
