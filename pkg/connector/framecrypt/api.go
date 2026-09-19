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

package framecrypt

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// IdentityKeyMode is ZenonIdentityStoreInterface.ZenonIdentityKeyMode.
type IdentityKeyMode int32

const (
	// IdentityKeyRandom is what ZenonAlwaysTrustIdentityStore uses (a fresh
	// key pair per call, remote keys never validated).
	IdentityKeyRandom IdentityKeyMode = 0
	// IdentityKeyPersistent uses the device's Signal identity key.
	IdentityKeyPersistent IdentityKeyMode = 1
	// IdentityKeyPersistentValidate is ZenonMAWIdentityStore's mode (E2EE
	// threads): persistent keys, and remote keys are checked against the
	// identity store.
	IdentityKeyPersistentValidate IdentityKeyMode = 2
)

// FrameDataHandlerType is FrameDataHandlerTypeTypes.FrameDataHandlerType:
// how much of a frame stays in the clear as codec header.
type FrameDataHandlerType int32

const (
	HandlerGeneric FrameDataHandlerType = 0
	HandlerH265    FrameDataHandlerType = 2
	HandlerH264    FrameDataHandlerType = 3
	HandlerAV1     FrameDataHandlerType = 4
)

// HandlerForCodec mirrors ZenonSecureFrameManager.$14 for the encryptor:
// audio tracks are always Generic; video is H264 when the negotiated codec
// name contains "H264" (case-sensitive, like the JS includes()),
// otherwise Generic (VP8, VP9, ...).
func HandlerForCodec(audio bool, codec string) FrameDataHandlerType {
	if !audio && strings.Contains(codec, "H264") {
		return HandlerH264
	}
	return HandlerGeneric
}

// DecryptorHandlers mirrors ZenonSecureFrameManager.$18: the handler types a
// decryptor accepts before the codec is known.
func DecryptorHandlers(audio bool) []FrameDataHandlerType {
	if audio {
		return []FrameDataHandlerType{HandlerGeneric}
	}
	return []FrameDataHandlerType{HandlerGeneric, HandlerH264}
}

// FrameError is a SecureFrameErrors code: the per-frame error of the
// module's SecureFrame layer.
type FrameError int32

// SecureFrameErrors codes.
const (
	ErrAlloc                 FrameError = 1
	ErrInvalidParam          FrameError = 2
	ErrCipher                FrameError = 3
	ErrParse                 FrameError = 4
	ErrInvalidKey            FrameError = 5
	ErrMissingKey            FrameError = 6
	ErrOutOfRatchetSpace     FrameError = 7
	ErrCipherAuth            FrameError = 8
	ErrFrameTooOld           FrameError = 9
	ErrSeenFrame             FrameError = 10
	ErrInvalidFrame          FrameError = 11
	ErrSettingInvalidKey     FrameError = 12
	ErrSettingExistingKey    FrameError = 13
	ErrEscapeData            FrameError = 14
	ErrDeescapeData          FrameError = 15
	ErrParseFrameOrKey       FrameError = 16
	ErrSetKeyInvalidIndex    FrameError = 17
	ErrUnsupportedCodec      FrameError = 18
	ErrStateSyncProviderNSet FrameError = 19
)

var frameErrorNames = map[FrameError]string{
	0: "SUCCESS", 1: "ERROR_ALLOC", 2: "ERROR_INVALIDPARAM", 3: "ERROR_CIPHER",
	4: "ERROR_PARSE", 5: "ERROR_INVALID_KEY", 6: "ERROR_MISSING_KEY",
	7: "ERROR_OUT_OF_RATCHET_SPACE", 8: "ERROR_CIPHER_AUTH", 9: "ERROR_FRAME_TOO_OLD",
	10: "ERROR_SEEN_FRAME", 11: "ERROR_INVALID_FRAME", 12: "ERROR_SETTING_INVALID_KEY",
	13: "ERROR_SETTING_EXISTING_KEY", 14: "ERROR_ESCAPE_DATA", 15: "ERROR_DEESCAPE_DATA",
	16: "ERROR_PARSE_FRAME_OR_KEY", 17: "ERROR_SET_KEY_INVALID_INDEX",
	18: "ERROR_UNSUPPORTED_CODEC", 19: "ERROR_STATE_SYNC_PROVIDER_NOT_SET",
}

func (e FrameError) Error() string {
	if n, ok := frameErrorNames[e]; ok {
		return "framecrypt: " + n
	}
	return fmt.Sprintf("framecrypt: secure frame error %d", int32(e))
}

// IdentityStore is the proxy identity store the module calls into
// (FrameEncryptionWasm P(), backed on the web by
// ZenonIdentityStoreInMemoryCache). The functions run synchronously inside
// a module call with the Instance locked; they must not call the Instance.
type IdentityStore struct {
	// LocalUserID is the local actor id (ZenonActor.getID()).
	LocalUserID string
	// LocalDeviceID is the Signal device id.
	LocalDeviceID int32
	// KeyMode is the local identity key mode.
	KeyMode IdentityKeyMode
	// LocalIdentityKey returns the Signal identity key pair: the 33-byte
	// serialized public key (0x05 || curve25519) and the 32-byte private
	// key (WASignalKeys.makeSerializedKeyPair).
	LocalIdentityKey func() (serializedPub, priv []byte)
	// RemoteIdentityKey returns the 33-byte serialized identity key the
	// store holds for a user's device, or nil if unknown.
	RemoteIdentityKey func(userID string, deviceID int32) []byte
	// SaveRemoteIdentityKey stores a key the module learned (optional).
	SaveRemoteIdentityKey func(userID string, deviceID int32, serializedPub []byte)
}

// ContextConfig configures an E2eeContext.
type ContextConfig struct {
	// E2eeMandated is true for calls in end-to-end encrypted threads.
	E2eeMandated bool
	Identity     IdentityStore
	// OnE2eeModelUpdate receives the module's serialized E2eeModel (Thrift
	// compact, E2eeModelSerializers) whenever it changes. Optional.
	OnE2eeModelUpdate func(model []byte)
}

// E2eeContext is the module's e2eeContext (FrameEncryptionWasm h()).
type E2eeContext struct {
	in    *Instance
	ptr   uint32
	slots []uint32
}

// NewContext creates an e2eeContext with a proxy identity store.
func (in *Instance) NewContext(ctx context.Context, cfg ContextConfig) (*E2eeContext, error) {
	if cfg.Identity.LocalIdentityKey == nil {
		return nil, errors.New("framecrypt: IdentityStore.LocalIdentityKey is required")
	}
	c := &E2eeContext{in: in}
	err := in.do(ctx, func(ctx context.Context) error {
		id := cfg.Identity
		getLocal, err := in.allocSlot("i", func(ctx context.Context, st []uint64) {
			pub, priv := id.LocalIdentityKey()
			pp := in.mustMalloc(ctx, pub)
			kp := in.mustMalloc(ctx, priv)
			st[0] = uint64(in.mustCall(ctx, "identityKey_create", uint64(pp), uint64(len(pub)), uint64(kp), uint64(len(priv))))
			in.free(ctx, pp, kp)
		})
		if err != nil {
			return err
		}
		c.slots = append(c.slots, getLocal)
		getRemote, err := in.allocSlot("iiii", func(ctx context.Context, st []uint64) {
			user := string(in.read(uint32(st[0]), uint32(st[1])))
			var key []byte
			if id.RemoteIdentityKey != nil {
				key = id.RemoteIdentityKey(user, int32(st[2]))
			}
			kp := in.mustMalloc(ctx, key)
			st[0] = uint64(in.mustCall(ctx, "publicKey_create", uint64(kp), uint64(len(key))))
			in.free(ctx, kp)
		})
		if err != nil {
			return err
		}
		c.slots = append(c.slots, getRemote)
		save, err := in.allocSlot("iiiiii", func(ctx context.Context, st []uint64) {
			user := string(in.read(uint32(st[0]), uint32(st[1])))
			key := in.read(uint32(st[3]), uint32(st[4]))
			if id.SaveRemoteIdentityKey != nil {
				id.SaveRemoteIdentityKey(user, int32(st[2]), key)
			}
			st[0] = 0
		})
		if err != nil {
			return err
		}
		c.slots = append(c.slots, save)
		devID, err := in.allocSlot("i", func(_ context.Context, st []uint64) {
			st[0] = uint64(uint32(id.LocalDeviceID))
		})
		if err != nil {
			return err
		}
		c.slots = append(c.slots, devID)
		model, err := in.allocSlot("vi", func(ctx context.Context, st []uint64) {
			if cfg.OnE2eeModelUpdate == nil {
				return
			}
			data, err := in.byteBuffer(ctx, uint32(st[0]))
			if err != nil {
				panic(err)
			}
			in.deferEvent(func() { cfg.OnE2eeModelUpdate(data) })
		})
		if err != nil {
			return err
		}
		c.slots = append(c.slots, model)

		deps, err := in.call32(ctx, "proxyIdentityStoreDeps_create")
		if err != nil {
			return err
		}
		defer func() { _, _ = in.call(ctx, "proxyIdentityStoreDeps_free", uint64(deps)) }()
		log := uint64(in.logSlot)
		if _, err = in.call(ctx, "proxyIdentityStoreDeps_setGetLocalPublicPrivateIdentityKeyCallback", uint64(deps), uint64(getLocal), log); err != nil {
			return err
		}
		if _, err = in.call(ctx, "proxyIdentityStoreDeps_setGetRemotePublicIdentityKeyCallback", uint64(deps), uint64(getRemote), log); err != nil {
			return err
		}
		if _, err = in.call(ctx, "proxyIdentityStoreDeps_setSaveRemotePublicIdentityKeyCallback", uint64(deps), uint64(save)); err != nil {
			return err
		}
		if _, err = in.call(ctx, "proxyIdentityStoreDeps_setGetLocalDeviceIdCallback", uint64(deps), uint64(devID)); err != nil {
			return err
		}
		err = in.withBytes(ctx, []byte(id.LocalUserID), func(p, n uint32) error {
			_, err := in.call(ctx, "proxyIdentityStoreDeps_setLocalUserId", uint64(deps), uint64(p), uint64(n))
			return err
		})
		if err != nil {
			return err
		}
		if _, err = in.call(ctx, "proxyIdentityStoreDeps_setLocalIdentityKeyMode", uint64(deps), uint64(uint32(id.KeyMode))); err != nil {
			return err
		}
		c.ptr, err = in.call32(ctx, "e2eeContext_create", b2u(cfg.E2eeMandated), log, uint64(model), uint64(deps))
		if err == nil && c.ptr == 0 {
			err = errors.New("framecrypt: e2eeContext_create returned null")
		}
		return err
	})
	if err != nil {
		in.mu.Lock()
		in.freeSlots(c.slots...)
		in.mu.Unlock()
		return nil, err
	}
	return c, nil
}

// Close frees the context. Keys managers created from it must be closed
// first.
func (c *E2eeContext) Close(ctx context.Context) error {
	return c.in.do(ctx, func(ctx context.Context) error {
		if c.ptr == 0 {
			return nil
		}
		_, err := c.in.call(ctx, "e2eeContext_free", uint64(c.ptr))
		c.ptr = 0
		c.in.freeSlots(c.slots...)
		return err
	})
}

func (in *Instance) mustMalloc(ctx context.Context, b []byte) uint32 {
	p, err := in.malloc(ctx, b)
	if err != nil {
		panic(err)
	}
	return p
}

func b2u(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

// KeysManagerConfig configures an EncryptionKeysManager (FrameEncryptionWasm y()).
type KeysManagerConfig struct {
	// UserID is the local actor id (encryptionKeysManagerDeps_setUserIdStr).
	UserID string
	// E2eeMandated is isE2eeMandated || isE2eeInfraMandated.
	E2eeMandated bool
	// InfraMandated is ZenonE2eeMandatedStateManager.isInfraE2eeMandated():
	// when E2EE negotiation is off, ZenonSecureFrameManager.encrypt drops
	// frames (returns an empty buffer) if it is set and sends them
	// unencrypted otherwise. Encrypt returns ErrEncryptionOff for the
	// former.
	InfraMandated bool
	// SendE2eeMessage must deliver data as a DATA_MESSAGE with topic
	// "E2eeKey" to recipient (ZenonE2eeCore sendE2eeMessageFn:
	// sendGenericDataMessage("E2eeKey", data, [recipient], [])). Called
	// after the API call that produced it returns.
	SendE2eeMessage func(recipient string, data []byte)
	// SctpSendE2eeMessage is the media-datachannel variant (topic
	// E2eeKeyMediaChannel; the glue passes it as a latin-1 string).
	// Optional: when nil such messages are dropped with a warning, so
	// don't call OnMediaDataChannelReady without it.
	SctpSendE2eeMessage func(recipient string, data []byte)
	// ManualKeyIndex turns off the automatic sender key index updates.
	// By default the manager does what ZenonEncryptionKeysManager.$11
	// does: after a successful server update or processed message that
	// returns a key index other than "" or "0", call UpdateSenderKeyIndex
	// after keyUpdateDelay (a Go timer) and repeat with the index it
	// returns; a server update that turns E2EE off cancels the chain
	// ($10). With ManualKeyIndex the caller must call
	// UpdateSenderKeyIndex itself, or new keys (e.g. the rekey after a
	// participant leaves) are never used for sending.
	ManualKeyIndex bool
}

// KeysManager is the module's encryptionKeysManager, plus the E2EE on/off
// state ZenonSecureFrameManager keeps for its encryptors and decryptors.
type KeysManager struct {
	in      *Instance
	ptr     uint32
	slots   []uint32
	follow  bool
	pending []*time.Timer

	infraMandated bool
	// encDisabled/decDisabled are ZenonSecureFrameManager $5/$6.
	encDisabled bool
	decDisabled bool
	decTimer    *time.Timer
	decryptors  map[*FrameDecryptor]struct{}
	// errCounters is the last seen value of the frame error counters of
	// the module's GroupE2eeMetrics (see frameErrorCounters).
	errCounters map[int16]int64
}

// NewKeysManager creates an encryptionKeysManager bound to c.
func (c *E2eeContext) NewKeysManager(ctx context.Context, cfg KeysManagerConfig) (*KeysManager, error) {
	in := c.in
	km := &KeysManager{in: in, follow: !cfg.ManualKeyIndex, infraMandated: cfg.InfraMandated,
		decryptors: map[*FrameDecryptor]struct{}{}}
	err := in.do(ctx, func(ctx context.Context) error {
		deps, err := in.call32(ctx, "encryptionKeysManagerDeps_create")
		if err != nil {
			return err
		}
		defer func() { _, _ = in.call(ctx, "encryptionKeysManagerDeps_free", uint64(deps)) }()
		if _, err = in.call(ctx, "encryptionKeysManagerDeps_setE2eeContext", uint64(deps), uint64(c.ptr)); err != nil {
			return err
		}
		send, err := in.allocSlot("viiii", func(_ context.Context, st []uint64) {
			to := string(in.read(uint32(st[0]), uint32(st[1])))
			data := in.read(uint32(st[2]), uint32(st[3]))
			if cfg.SendE2eeMessage != nil {
				in.deferEvent(func() { cfg.SendE2eeMessage(to, data) })
			}
		})
		if err != nil {
			return err
		}
		km.slots = append(km.slots, send)
		if _, err = in.call(ctx, "encryptionKeysManagerDeps_setSendE2eeMessageCallback", uint64(deps), uint64(send)); err != nil {
			return err
		}
		if _, err = in.call(ctx, "encryptionKeysManagerDeps_setIsSessionKeyEnabled", uint64(deps), 1); err != nil {
			return err
		}
		// The glue installs the SCTP sender and the timer callbacks only
		// when it is given an SCTP sender, but encryptionKeysManager_create
		// builds the media-channel key sender only when all three are set
		// (deps offsets 32, 24, 28), and processE2eeServerUpdate calls
		// through it unconditionally: without them the first server update
		// traps with "invalid table access" (a virtual call on a null
		// object). The web client always has one in SFU calls, so always
		// install all three.
		sctp, err := in.allocSlot("viiii", func(_ context.Context, st []uint64) {
			to := string(in.read(uint32(st[0]), uint32(st[1])))
			data := in.read(uint32(st[2]), uint32(st[3]))
			if cfg.SctpSendE2eeMessage != nil {
				in.deferEvent(func() { cfg.SctpSendE2eeMessage(to, data) })
			} else {
				in.deferLog("WARNING", fmt.Sprintf("framecrypt: dropped an E2eeKeyMediaChannel message to %s (no SctpSendE2eeMessage)", to))
			}
		})
		if err != nil {
			return err
		}
		km.slots = append(km.slots, sctp)
		if _, err = in.call(ctx, "encryptionKeysManagerDeps_setSctpSendE2eeMessageCallback", uint64(deps), uint64(sctp)); err != nil {
			return err
		}
		start, err := in.allocSlot("iii", func(_ context.Context, st []uint64) {
			st[0] = uint64(uint32(in.startTimer(int64(int32(st[0])), uint32(st[1]))))
		})
		if err != nil {
			return err
		}
		km.slots = append(km.slots, start)
		if _, err = in.call(ctx, "encryptionKeysManagerDeps_setStartTimerCallback", uint64(deps), uint64(start)); err != nil {
			return err
		}
		stop, err := in.allocSlot("vi", func(_ context.Context, st []uint64) {
			in.stopTimer(int32(st[0]))
		})
		if err != nil {
			return err
		}
		km.slots = append(km.slots, stop)
		if _, err = in.call(ctx, "encryptionKeysManagerDeps_setStopTimerCallback", uint64(deps), uint64(stop)); err != nil {
			return err
		}
		err = in.withBytes(ctx, []byte(cfg.UserID), func(p, n uint32) error {
			_, err := in.call(ctx, "encryptionKeysManagerDeps_setUserIdStr", uint64(deps), uint64(p), uint64(n))
			return err
		})
		if err != nil {
			return err
		}
		if _, err = in.call(ctx, "encryptionKeysManagerDeps_setIsE2eeInfraMandated", uint64(deps), b2u(cfg.E2eeMandated)); err != nil {
			return err
		}
		km.ptr, err = in.call32(ctx, "encryptionKeysManager_create", uint64(deps))
		if err == nil && km.ptr == 0 {
			err = errors.New("framecrypt: encryptionKeysManager_create returned null")
		}
		return err
	})
	if err != nil {
		in.mu.Lock()
		in.freeSlots(km.slots...)
		in.mu.Unlock()
		return nil, err
	}
	return km, nil
}

// Close frees the keys manager and cancels its key index updates.
func (km *KeysManager) Close(ctx context.Context) error {
	return km.in.do(ctx, func(ctx context.Context) error {
		km.cancelKeyIndexUpdates()
		if km.decTimer != nil {
			km.decTimer.Stop()
			km.decTimer = nil
		}
		if km.ptr == 0 {
			return nil
		}
		_, err := km.in.call(ctx, "encryptionKeysManager_free", uint64(km.ptr))
		km.ptr = 0
		km.in.freeSlots(km.slots...)
		return err
	})
}

// SetLocalE2eeID sets the local E2EE id, "<actorId>:<cname of the local
// SDP's media>" (ZenonEncryptionKeysManager.onLocalSdpSet / $9).
func (km *KeysManager) SetLocalE2eeID(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("framecrypt: local e2ee id cannot be empty")
	}
	return km.in.do(ctx, func(ctx context.Context) error {
		return km.in.withBytes(ctx, []byte(id), func(p, n uint32) error {
			_, err := km.in.call(ctx, "encryptionKeysManager_setLocalE2eeId", uint64(km.ptr), uint64(p), uint64(n))
			return err
		})
	})
}

// SerializedE2eeClientState returns the E2eeClientState (Thrift compact)
// the client publishes as the "E2eeState" state-sync value of its JOIN.
func (km *KeysManager) SerializedE2eeClientState(ctx context.Context) ([]byte, error) {
	var out []byte
	err := km.in.do(ctx, func(ctx context.Context) error {
		in := km.in
		r, err := in.call32(ctx, "encryptionKeysManager_getSerializedE2eeClientState", uint64(km.ptr))
		if err != nil {
			return err
		}
		defer func() { _, _ = in.call(ctx, "dataResult_free", uint64(r)) }()
		code, err := in.call32(ctx, "dataResult_getErrCode", uint64(r))
		if err != nil {
			return err
		}
		if code != 0 {
			return fmt.Errorf("framecrypt: getSerializedE2eeClientState: error code %d", int32(code))
		}
		p, err := in.call32(ctx, "dataResult_getData", uint64(r))
		if err != nil {
			return err
		}
		n, err := in.call32(ctx, "dataResult_getDataSize", uint64(r))
		if err != nil {
			return err
		}
		out = in.read(p, n)
		return nil
	})
	return out, err
}

// ServerUpdateResult is the result of ProcessE2eeServerUpdate.
type ServerUpdateResult struct {
	// ErrorCode 0 means E2EE is negotiated on; anything else means off
	// (the web client then passes frames through unencrypted).
	ErrorCode                   int32
	KeyIndex                    string
	KeyUpdateDelay              time.Duration
	RemoveFrameDecryptorDelayMs int32
}

// ProcessE2eeServerUpdate feeds the server's E2eeServerState (the
// "E2eeState" state-sync value of JoinResponse / server media updates).
func (km *KeysManager) ProcessE2eeServerUpdate(ctx context.Context, state []byte) (*ServerUpdateResult, error) {
	res := &ServerUpdateResult{ErrorCode: -1}
	err := km.in.do(ctx, func(ctx context.Context) error {
		in := km.in
		return in.withBytes(ctx, state, func(p, n uint32) error {
			r, err := in.call32(ctx, "encryptionKeysManager_processE2eeServerUpdate", uint64(km.ptr), uint64(p), uint64(n))
			if err != nil {
				return err
			}
			defer func() {
				_, _ = in.call(ctx, "encryptionKeysManagerProcessE2eeServerUpdateResult_free", uint64(r))
			}()
			const pre = "encryptionKeysManagerProcessE2eeServerUpdateResult_"
			code, err := in.call32(ctx, pre+"getErrCode", uint64(r))
			if err != nil {
				return err
			}
			res.ErrorCode = int32(code)
			if res.KeyIndex, err = in.resultString(ctx, pre+"getKeyIndexDataPtr", pre+"getKeyIndexSize", r); err != nil {
				return err
			}
			delay, err := in.call32(ctx, pre+"getKeyUpdateDelay", uint64(r))
			if err != nil {
				return err
			}
			res.KeyUpdateDelay = time.Duration(int32(delay)) * time.Millisecond
			rm, err := in.call32(ctx, pre+"getRemoveFrameDecryptorDelayMs", uint64(r))
			res.RemoveFrameDecryptorDelayMs = int32(rm)
			if err != nil {
				return err
			}
			return km.applyNegotiation(ctx, res)
		})
	})
	return res, err
}

// applyNegotiation is ZenonEncryptionKeysManager.processE2eeServerState
// after the module call. Caller holds the lock.
func (km *KeysManager) applyNegotiation(ctx context.Context, res *ServerUpdateResult) error {
	if res.ErrorCode == 0 {
		if km.encDisabled {
			// Back on after being off. The module's existing frame
			// decryptors fail every frame from then on with
			// ERROR_INVALID_KEY while fresh ones decrypt the same frames
			// (TestE2eeOffPassthrough), so replace them, keeping the Go
			// handles valid.
			km.encDisabled = false
			for fd := range km.decryptors {
				if err := fd.recreate(ctx); err != nil {
					return err
				}
			}
		}
		km.encDisabled, km.decDisabled = false, false
		if km.decTimer != nil {
			km.decTimer.Stop()
			km.decTimer = nil
		}
		if km.follow {
			km.scheduleKeyIndex(res.KeyIndex, int32(res.KeyUpdateDelay/time.Millisecond))
		}
		return nil
	}
	// E2EE negotiated off: send unencrypted, accept unencrypted frames,
	// and after removeFrameDecryptorDelayMs stop decrypting altogether.
	km.encDisabled = true
	for fd := range km.decryptors {
		if _, err := km.in.call(ctx, "frameDecryptor_enableUnencryptedData", uint64(fd.ptr)); err != nil {
			return err
		}
	}
	if km.decTimer != nil {
		km.decTimer.Stop()
	}
	var t *time.Timer
	t = time.AfterFunc(time.Duration(max(res.RemoveFrameDecryptorDelayMs, 0))*time.Millisecond, func() {
		_ = km.in.do(context.Background(), func(context.Context) error {
			if km.decTimer == t {
				km.decDisabled = true
				km.decTimer = nil
			}
			return nil
		})
	})
	km.decTimer = t
	km.cancelKeyIndexUpdates()
	return nil
}

// E2EEEnabled reports whether the last server update negotiated E2EE on
// (frames are encrypted and decrypted).
func (km *KeysManager) E2EEEnabled() bool {
	km.in.mu.Lock()
	defer km.in.mu.Unlock()
	return !km.encDisabled
}

// MessageResult is the result of ProcessE2eeMessage.
type MessageResult struct {
	StatusCode     int32
	KeyIndex       string
	KeyUpdateDelay time.Duration
}

// ProcessE2eeMessage feeds the data of a received "E2eeKey" DATA_MESSAGE.
func (km *KeysManager) ProcessE2eeMessage(ctx context.Context, data []byte) (*MessageResult, error) {
	res := &MessageResult{StatusCode: -1}
	err := km.in.do(ctx, func(ctx context.Context) error {
		in := km.in
		return in.withBytes(ctx, data, func(p, n uint32) error {
			// The glue passes three arguments to this four-parameter export;
			// JS fills the fourth with undefined → 0.
			r, err := in.call32(ctx, "encryptionKeysManager_processE2eeMessage", uint64(km.ptr), uint64(p), uint64(n), 0)
			if err != nil {
				return err
			}
			defer func() {
				_, _ = in.call(ctx, "encryptionKeysManagerProcessE2eeMessageResult_free", uint64(r))
			}()
			const pre = "encryptionKeysManagerProcessE2eeMessageResult_"
			code, err := in.call32(ctx, pre+"getStatusCode", uint64(r))
			if err != nil {
				return err
			}
			res.StatusCode = int32(code)
			if res.KeyIndex, err = in.resultString(ctx, pre+"getKeyIndexDataPtr", pre+"getKeyIndexSize", r); err != nil {
				return err
			}
			delay, err := in.call32(ctx, pre+"getKeyUpdateDelay", uint64(r))
			res.KeyUpdateDelay = time.Duration(int32(delay)) * time.Millisecond
			if err == nil && km.follow {
				km.scheduleKeyIndex(res.KeyIndex, int32(delay))
			}
			return err
		})
	})
	return res, err
}

func (in *Instance) resultString(ctx context.Context, ptrFn, sizeFn string, r uint32) (string, error) {
	p, err := in.call32(ctx, ptrFn, uint64(r))
	if err != nil {
		return "", err
	}
	n, err := in.call32(ctx, sizeFn, uint64(r))
	if err != nil {
		return "", err
	}
	return string(in.read(p, n)), nil
}

// UpdateSenderKeyIndex activates keyIndex for sending and returns the next
// key index to activate (encryptionKeysManager_updateSenderKeyIndex).
func (km *KeysManager) UpdateSenderKeyIndex(ctx context.Context, keyIndex string) (string, error) {
	var next string
	err := km.in.do(ctx, func(ctx context.Context) (err error) {
		next, err = km.updateSenderKeyIndex(ctx, keyIndex)
		return
	})
	return next, err
}

func (km *KeysManager) updateSenderKeyIndex(ctx context.Context, keyIndex string) (string, error) {
	in := km.in
	var next string
	err := in.withBytes(ctx, []byte(keyIndex), func(p, n uint32) error {
		sb, err := in.call32(ctx, "encryptionKeysManager_updateSenderKeyIndex", uint64(km.ptr), uint64(p), uint64(n))
		if err != nil {
			return err
		}
		defer func() { _, _ = in.call(ctx, "stringBuffer_free", uint64(sb)) }()
		next, err = in.resultString(ctx, "stringBuffer_getDataPtr", "stringBuffer_getSize", sb)
		return err
	})
	return next, err
}

// OnMediaDataChannelReady tells the manager the media data channel is up.
func (km *KeysManager) OnMediaDataChannelReady(ctx context.Context) error {
	return km.in.do(ctx, func(ctx context.Context) error {
		_, err := km.in.call(ctx, "encryptionKeysManager_onMediaDataChannelReady", uint64(km.ptr))
		return err
	})
}

// scheduleKeyIndex is ZenonEncryptionKeysManager.$11. Caller holds the lock.
func (km *KeysManager) scheduleKeyIndex(keyIndex string, delayMs int32) {
	if keyIndex == "0" || keyIndex == "" || delayMs < 0 {
		return
	}
	var t *time.Timer
	t = time.AfterFunc(time.Duration(delayMs)*time.Millisecond, func() {
		_ = km.in.do(context.Background(), func(ctx context.Context) error {
			if km.ptr == 0 {
				return nil
			}
			for i, p := range km.pending {
				if p == t {
					km.pending = append(km.pending[:i], km.pending[i+1:]...)
					break
				}
			}
			next, err := km.updateSenderKeyIndex(ctx, keyIndex)
			if err == nil {
				km.scheduleKeyIndex(next, delayMs)
			}
			return err
		})
	})
	km.pending = append(km.pending, t)
}

func (km *KeysManager) cancelKeyIndexUpdates() {
	for _, t := range km.pending {
		t.Stop()
	}
	km.pending = nil
}

type timerEntry struct {
	t *time.Timer
}

// startTimer is the glue's startTimer callback: run
// wasmTimerAdapter_execute(handle) after delayMs and return an id for
// stopTimer. Caller holds the lock.
func (in *Instance) startTimer(delayMs int64, handle uint32) int32 {
	in.timerSeq++
	id := in.timerSeq
	e := &timerEntry{}
	e.t = time.AfterFunc(time.Duration(max(delayMs, 0))*time.Millisecond, func() {
		_ = in.do(context.Background(), func(ctx context.Context) error {
			if in.timers[id] != e {
				return nil
			}
			delete(in.timers, id)
			in.timersFired++
			_, err := in.call(ctx, "wasmTimerAdapter_execute", uint64(handle))
			return err
		})
	})
	in.timers[id] = e
	return id
}

func (in *Instance) stopTimer(id int32) {
	if e := in.timers[id]; e != nil {
		e.t.Stop()
		delete(in.timers, id)
		in.timersCancel++
	}
}

// SerializedE2eeMetrics returns the module's E2eeMetrics (Thrift compact,
// E2eeMetricsSerializers: field 2 is GroupE2eeMetrics), as the glue's
// getGroupE2eeMetrics reads it.
func (km *KeysManager) SerializedE2eeMetrics(ctx context.Context) ([]byte, error) {
	var out []byte
	err := km.in.do(ctx, func(ctx context.Context) (err error) {
		out, err = km.metrics(ctx)
		return
	})
	return out, err
}

func (km *KeysManager) metrics(ctx context.Context) ([]byte, error) {
	in := km.in
	bb, err := in.call32(ctx, "encryptionKeysManager_getSerializedE2eeMetrics", uint64(km.ptr))
	if err != nil {
		return nil, err
	}
	defer func() { _, _ = in.call(ctx, "byteBuffer_free", uint64(bb)) }()
	return in.byteBuffer(ctx, bb)
}
