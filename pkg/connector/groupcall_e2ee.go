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

// End-to-end encrypted group calls: Messenger group calls in E2EE threads wrap every media frame in
// SFrame, with keys negotiated inside Meta's frame-encryption WebAssembly module. The bridge runs that
// module (package framecrypt) once per call, as the web client's ZenonE2eeCore,
// ZenonEncryptionKeysManager and ZenonSecureFrameManager do: the module's E2eeClientState goes into the
// JOIN; the server's E2eeServerState (JoinResponse and media updates) and the peers' "E2eeKey"
// DATA_MESSAGEs are fed to it; its own E2eeKey messages go out as DATA_MESSAGEs; and every frame is
// decrypted on its way to LiveKit, and the Matrix user's audio encrypted on its way to Messenger.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/libsignal/ecc"
	waTypes "go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2/callbridge"

	"go.mau.fi/mautrix-meta/pkg/connector/framecrypt"
	"go.mau.fi/mautrix-meta/pkg/connector/metacall"
	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
)

// frameCryptFallbackURL is the frame-encryption module the web client loaded when this was written
// (bxData "37" of the group-call page). It is used when the page doesn't name one.
const frameCryptFallbackURL = "https://static.xx.fbcdn.net/rsrc.php/yV/r/Wfd6t0e74Jr.wasm"

// frameCryptLoader holds the compiled frame-encryption module, loaded on the first encrypted call.
type frameCryptLoader struct {
	lock sync.Mutex
	rt   *framecrypt.Runtime
}

// frameCryptRuntime returns the compiled frame-encryption module, loading it on first use from the
// configured source, the URL the thread's group-call page names, or the last known URL.
func (m *MetaClient) frameCryptRuntime(ctx context.Context, threadID string) (*framecrypt.Runtime, error) {
	l := &m.Main.frameCrypt
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.rt != nil {
		return l.rt, nil
	}
	log := m.Main.Bridge.Log.With().Str("component", "framecrypt").Logger()
	var sources []string
	if src := m.Main.Config.CallBridgingFrameEncryptionModule; src != "" {
		sources = append(sources, src)
	} else if u, err := m.Client.FetchFrameEncryptionModuleURL(ctx, threadID); err != nil {
		log.Warn().Err(err).Msg("Couldn't find the frame-encryption module on the group call page, trying the last known URL")
	} else {
		sources = append(sources, u)
	}
	if len(sources) == 0 || sources[0] != frameCryptFallbackURL {
		sources = append(sources, frameCryptFallbackURL)
	}
	var errs []error
	for _, src := range sources {
		start := time.Now()
		var wasm []byte
		var err error
		if strings.Contains(src, "://") {
			wasm, err = m.Client.FetchStaticResource(ctx, src)
		} else {
			wasm, err = os.ReadFile(src)
		}
		var rt *framecrypt.Runtime
		if err == nil {
			rt, err = framecrypt.NewRuntime(context.WithoutCancel(ctx), wasm, &framecrypt.Options{Log: frameCryptLog(log)})
		}
		if err != nil {
			log.Warn().Err(err).Str("source", src).Msg("Failed to load the frame-encryption module")
			errs = append(errs, fmt.Errorf("%s: %w", src, err))
			continue
		}
		log.Info().Str("source", src).Int("size", len(wasm)).Dur("took", time.Since(start)).
			Msg("Loaded Messenger's frame-encryption module")
		l.rt = rt
		return rt, nil
	}
	return nil, errors.Join(errs...)
}

// frameCryptLog passes the module's own log lines on. Its errors are mostly expected ones (frames
// that arrive before their key), so they are warnings here.
func frameCryptLog(log zerolog.Logger) framecrypt.LogFunc {
	return func(level, msg string) {
		switch level {
		case "ERROR":
			log.Warn().Str("module_level", level).Msg(msg)
		case "WARNING":
			log.Info().Str("module_level", level).Msg(msg)
		default:
			log.Debug().Str("module_level", level).Msg(msg)
		}
	}
}

// groupE2ee is one encrypted group call's instance of the frame-encryption module.
type groupE2ee struct {
	ctx    context.Context
	cancel context.CancelFunc
	cfg    groupE2eeConfig
	log    zerolog.Logger
	in     *framecrypt.Instance
	ec     *framecrypt.E2eeContext
	km     *framecrypt.KeysManager
	enc    *framecrypt.FrameEncryptor

	// out queues E2eeKey messages for the sender goroutine, which keeps their order.
	out chan e2eeKeyOut

	lock sync.Mutex
	// peerKeys are the identity keys the server's endpointInfos give, by "<userId>.<deviceId>"
	// (the web client's ZenonIdentityStoreInMemoryCache).
	peerKeys map[string][]byte
	// untrusted are the users with a device whose identity key TrustIdentity rejected.
	untrusted map[string]bool
	closed    bool
}

type groupE2eeConfig struct {
	// Mandated is set for calls in end-to-end encrypted chats: media is dropped rather than sent
	// in the clear if encryption is negotiated off. Other calls encrypt whenever Messenger
	// negotiates encryption on (as the web client does) and pass media through otherwise.
	Mandated bool
	SelfID   string
	Identity *metacall.Identity
	// LocalCname is the cname of our SDP; our E2EE id is "<SelfID>:<LocalCname>".
	LocalCname string
	// Send delivers an E2eeKey message to a user. It is called from one goroutine, in order.
	Send func(ctx context.Context, to string, data []byte) error
	// TrustIdentity reports whether a peer device's identity key (from the server) may be used.
	// Optional. The module itself doesn't enforce identity keys (it saves whatever the server
	// sends), so a rejected user gets none of our keys, and theirs are ignored.
	TrustIdentity func(ctx context.Context, userID string, deviceID int32, key []byte) bool
	Log           zerolog.Logger
}

type e2eeKeyOut struct {
	to   string
	data []byte
}

// newGroupE2ee sets up a call's encryption and returns our E2eeClientState for the JOIN.
func newGroupE2ee(ctx context.Context, rt *framecrypt.Runtime, cfg groupE2eeConfig) (*groupE2ee, []byte, error) {
	if cfg.Identity == nil {
		return nil, nil, errors.New("no Messenger encryption device")
	} else if cfg.LocalCname == "" {
		return nil, nil, errors.New("local SDP has no cname")
	}
	e := &groupE2ee{
		cfg:       cfg,
		log:       cfg.Log,
		out:       make(chan e2eeKeyOut, 64),
		peerKeys:  map[string][]byte{},
		untrusted: map[string]bool{},
	}
	e.ctx, e.cancel = context.WithCancel(ctx)
	ok := false
	defer func() {
		if !ok {
			e.close()
		}
	}()
	var err error
	if e.in, err = rt.NewInstance(ctx); err != nil {
		return nil, nil, err
	}
	pub := append([]byte{ecc.DjbType}, cfg.Identity.Pub[:]...)
	priv := append([]byte(nil), cfg.Identity.Priv[:]...)
	e.ec, err = e.in.NewContext(ctx, framecrypt.ContextConfig{
		E2eeMandated: cfg.Mandated,
		Identity: framecrypt.IdentityStore{
			LocalUserID:       cfg.SelfID,
			LocalDeviceID:     cfg.Identity.DeviceID,
			KeyMode:           framecrypt.IdentityKeyPersistentValidate,
			LocalIdentityKey:  func() ([]byte, []byte) { return pub, priv },
			RemoteIdentityKey: e.remoteIdentityKey,
			SaveRemoteIdentityKey: func(userID string, deviceID int32, _ []byte) {
				e.log.Debug().Str("user", userID).Int32("device", deviceID).Msg("Frame-encryption module learned an identity key")
			},
		},
	})
	if err != nil {
		return nil, nil, err
	}
	e.km, err = e.ec.NewKeysManager(ctx, framecrypt.KeysManagerConfig{
		UserID:          cfg.SelfID,
		E2eeMandated:    cfg.Mandated,
		InfraMandated:   cfg.Mandated,
		SendE2eeMessage: e.queue,
		// Keys go over signalling only: the bridge never reports a media data channel as ready.
		SctpSendE2eeMessage: func(to string, _ []byte) {
			e.log.Trace().Str("to", to).Msg("Not sending an E2eeKey message over the media data channel")
		},
	})
	if err != nil {
		return nil, nil, err
	}
	if err = e.km.SetLocalE2eeID(ctx, cfg.SelfID+":"+cfg.LocalCname); err != nil {
		return nil, nil, err
	}
	if e.enc, err = e.km.NewFrameEncryptor(ctx); err != nil {
		return nil, nil, err
	}
	state, err := e.km.SerializedE2eeClientState(ctx)
	if err != nil {
		return nil, nil, err
	}
	ok = true
	go e.sendLoop()
	e.log.Info().Str("cname", cfg.LocalCname).Int("client_state_len", len(state)).Msg("Set up end-to-end encryption for the group call")
	return e, state, nil
}

// startE2ee sets up the call's encryption for our local SDP and returns our E2eeClientState.
func (g *groupCall) startE2ee(localSDP string) ([]byte, error) {
	rt, err := g.m.frameCryptRuntime(g.ctx, g.roomID())
	if err != nil {
		return nil, fmt.Errorf("load frame-encryption module: %w", err)
	}
	g.lock.Lock()
	mandated := g.e2ee
	g.lock.Unlock()
	e, state, err := newGroupE2ee(g.ctx, rt, groupE2eeConfig{
		Mandated:      mandated,
		SelfID:        strconv.FormatInt(g.m.selfFBID(), 10),
		Identity:      g.m.callIdentity(),
		LocalCname:    callbridge.SSRCCname(localSDP),
		Send:          g.sendE2eeKey,
		TrustIdentity: g.m.trustCallIdentity,
		Log:           g.log.With().Str("component", "e2ee").Logger(),
	})
	if err != nil {
		return nil, err
	}
	g.lock.Lock()
	g.crypt = e
	g.lock.Unlock()
	return state, nil
}

// sendE2eeKey sends an E2eeKey DATA_MESSAGE, once the JOIN gave the call its context.
func (g *groupCall) sendE2eeKey(ctx context.Context, to string, data []byte) error {
	for {
		g.lock.Lock()
		cc := g.cc
		g.lock.Unlock()
		if cc != nil {
			_, err := g.cb.sig.Request(ctx, cc.NewE2eeKeyMessage(to, data))
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// trustCallIdentity checks a call participant's identity key against whatsmeow's identity store:
// a device it holds a different key for is not trusted; unknown devices are.
func (m *MetaClient) trustCallIdentity(ctx context.Context, userID string, deviceID int32, key []byte) bool {
	dev := m.WADevice
	if dev == nil || dev.Identities == nil || len(key) != 33 {
		return true
	}
	addr := waTypes.JID{User: userID, Device: uint16(deviceID), Server: waTypes.MessengerServer}.SignalAddress().String()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	trusted, err := dev.Identities.IsTrustedIdentity(ctx, addr, [32]byte(key[1:]))
	if err != nil {
		m.UserLogin.Log.Warn().Err(err).Msg("Couldn't check a call participant's identity key")
		return true
	}
	return trusted
}

// remoteIdentityKey gives the module a peer device's identity key, the one the server's
// endpointInfos carried. It runs inside a module call and must not call the instance.
func (e *groupE2ee) remoteIdentityKey(userID string, deviceID int32) []byte {
	e.lock.Lock()
	defer e.lock.Unlock()
	return e.peerKeys[userID+"."+strconv.Itoa(int(deviceID))]
}

// isUntrusted reports whether a user's identity key was rejected.
func (e *groupE2ee) isUntrusted(userID string) bool {
	e.lock.Lock()
	defer e.lock.Unlock()
	return e.untrusted[userID]
}

// serverState feeds the server's E2eeServerState to the module, first caching the participants'
// identity keys from it.
func (e *groupE2ee) serverState(store rtcsignal.StateStore, from string) {
	st, ok := store.Get(rtcsignal.TopicE2eeState)
	if !ok || len(st.Data) == 0 {
		return
	}
	endpoints := 0
	if ss, err := rtcsignal.ParseE2eeServerState(st.Data); err != nil {
		e.log.Warn().Err(err).Str("from", from).Msg("Couldn't parse the server's E2EE state")
	} else {
		for key, ep := range ss.EndpointInfos {
			id, ok := rtcsignal.EndpointUserID(key)
			if !ok {
				continue
			}
			b, err := rtcsignal.ParsePreKeyBundle(ep.PreKeyBundle)
			if err != nil || len(b.IdentityKey) != 33 {
				e.log.Warn().Err(err).Str("endpoint", key).Msg("Call participant without a usable identity key")
				continue
			}
			uid := strconv.FormatInt(id, 10)
			trusted := e.cfg.TrustIdentity == nil || e.cfg.TrustIdentity(e.ctx, uid, ep.DeviceID, b.IdentityKey)
			e.lock.Lock()
			e.peerKeys[uid+"."+strconv.Itoa(int(ep.DeviceID))] = b.IdentityKey
			newlyUntrusted := !trusted && !e.untrusted[uid]
			if !trusted {
				e.untrusted[uid] = true
			}
			e.lock.Unlock()
			if newlyUntrusted {
				e.log.Warn().Str("user", uid).Int32("device", ep.DeviceID).
					Msg("A call participant's identity key doesn't match the identity store; not exchanging keys with them")
			}
			endpoints++
		}
	}
	e.log.Debug().Str("from", from).Hex("state", st.Data).Msg("Server E2EE state")
	res, err := e.km.ProcessE2eeServerUpdate(e.ctx, st.Data)
	if err != nil {
		e.log.Err(err).Str("from", from).Msg("Frame-encryption module failed on the server's E2EE state")
		return
	}
	e.log.Info().Str("from", from).Int("endpoints", endpoints).Int32("error_code", res.ErrorCode).
		Bool("key_index", res.KeyIndex != "").Dur("key_update_delay", res.KeyUpdateDelay).
		Msg("Processed the server's E2EE state")
	if res.ErrorCode != 0 && e.cfg.Mandated {
		e.log.Warn().Int32("error_code", res.ErrorCode).Msg("End-to-end encryption was negotiated off in an encrypted chat; media is dropped until it's back on")
	} else if res.ErrorCode != 0 {
		e.log.Info().Int32("error_code", res.ErrorCode).Msg("End-to-end encryption negotiated off; media goes through unencrypted")
	}
}

// keyMessage feeds a peer's E2eeKey DATA_MESSAGE to the module.
func (e *groupE2ee) keyMessage(dm *rtcsignal.DataMessage) {
	if e.isUntrusted(dm.Sender) {
		e.log.Debug().Str("sender", dm.Sender).Msg("Ignoring an E2eeKey message from an untrusted participant")
		return
	}
	res, err := e.km.ProcessE2eeMessage(e.ctx, dm.Data)
	if err != nil {
		e.log.Err(err).Str("sender", dm.Sender).Msg("Frame-encryption module failed on an E2eeKey message")
		return
	}
	e.log.Debug().Str("sender", dm.Sender).Int("len", len(dm.Data)).Int32("status", res.StatusCode).
		Bool("key_index", res.KeyIndex != "").Msg("Processed an E2eeKey message")
}

// queue takes an E2eeKey message from the module (called outside the instance lock).
func (e *groupE2ee) queue(to string, data []byte) {
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.closed {
		return
	}
	select {
	case e.out <- e2eeKeyOut{to: to, data: data}:
	default:
		e.log.Warn().Str("to", to).Msg("E2eeKey send queue full, dropping a message")
	}
}

// sendLoop sends the module's E2eeKey messages in order.
func (e *groupE2ee) sendLoop() {
	for {
		var msg e2eeKeyOut
		select {
		case <-e.ctx.Done():
			return
		case msg = <-e.out:
		}
		// The web client addresses them to the bare user id.
		to, _, _ := strings.Cut(msg.to, ":")
		if e.isUntrusted(to) {
			e.log.Debug().Str("to", to).Msg("Not sending an E2eeKey message to an untrusted participant")
			continue
		}
		if err := e.cfg.Send(e.ctx, to, msg.data); err != nil {
			if e.ctx.Err() == nil {
				e.log.Warn().Err(err).Str("to", to).Msg("Failed to send an E2eeKey message")
			}
		} else {
			e.log.Debug().Str("to", to).Int("len", len(msg.data)).Msg("Sent an E2eeKey message")
		}
	}
}

// encryptTransform encrypts the Matrix user's Opus frames for Messenger.
func (e *groupE2ee) encryptTransform(log zerolog.Logger) callbridge.FrameTransform {
	fl := newFrameLog(log, "encrypt")
	return func(frame []byte) ([]byte, error) {
		out, err := e.enc.Encrypt(e.ctx, framecrypt.HandlerGeneric, frame)
		fl.result(err)
		fl.sample(frame, out)
		return out, err
	}
}

// decryptor creates the decryptor of a remote participant's track.
func (e *groupE2ee) decryptor(e2eeID string, audio bool, log zerolog.Logger) (callbridge.FrameTransform, func(), error) {
	if uid, _, _ := strings.Cut(e2eeID, ":"); e.isUntrusted(uid) {
		return nil, nil, fmt.Errorf("%s's identity key doesn't match the identity store", uid)
	}
	fd, err := e.km.NewFrameDecryptor(e.ctx, e2eeID, framecrypt.DecryptorHandlers(audio))
	if err != nil {
		return nil, nil, err
	}
	fl := newFrameLog(log, "decrypt")
	xf := func(frame []byte) ([]byte, error) {
		out, err := fd.Decrypt(e.ctx, frame)
		fl.result(err)
		fl.sample(frame, out)
		return out, err
	}
	return xf, func() { _ = fd.Close(context.WithoutCancel(e.ctx)) }, nil
}

func (e *groupE2ee) close() {
	e.lock.Lock()
	e.closed = true
	e.lock.Unlock()
	e.cancel()
	ctx := context.Background()
	if e.enc != nil {
		_ = e.enc.Close(ctx)
	}
	if e.km != nil {
		_ = e.km.Close(ctx)
	}
	if e.ec != nil {
		_ = e.ec.Close(ctx)
	}
	if e.in != nil {
		_ = e.in.Close(ctx)
	}
}

// frameLog logs the first frame a transform handled and every change between success and a failure
// reason, so a stream's state (keys missing, then working) shows without a line per frame.
type frameLog struct {
	log  zerolog.Logger
	op   string
	lock sync.Mutex
	last string
	n    uint64
	logs int
}

// frameLogMax bounds the state changes logged per stream (a flapping stream would log every frame).
const frameLogMax = 20

func newFrameLog(log zerolog.Logger, op string) *frameLog {
	return &frameLog{log: log, op: op, last: "none"}
}

// sample logs the start of the first few frames a transform sees, which tells Opus (a TOC byte
// such as 0x78) from ciphertext.
func (fl *frameLog) sample(in, out []byte) {
	fl.lock.Lock()
	n := fl.n
	fl.lock.Unlock()
	// The first frames, then one every few seconds, so a stream that changes mid-call shows.
	if n > 5 && n%250 != 0 {
		return
	}
	fl.log.Debug().Str("op", fl.op).Uint64("frame", n).Int("in_len", len(in)).Hex("in", in[:min(len(in), 12)]).
		Int("out_len", len(out)).Hex("out", out[:min(len(out), 12)]).Msg("Frame sample")
}

func (fl *frameLog) result(err error) {
	state := "ok"
	if err != nil {
		state = err.Error()
	}
	fl.lock.Lock()
	defer fl.lock.Unlock()
	fl.n++
	if state == fl.last {
		return
	}
	prev := fl.last
	fl.last = state
	if fl.logs++; fl.logs > frameLogMax {
		return
	}
	ev := fl.log.Info()
	if err != nil {
		ev = fl.log.Warn().Err(err)
	}
	ev.Str("op", fl.op).Str("previous", prev).Uint64("frame", fl.n).Msg("Frame " + fl.op + " state changed")
}
