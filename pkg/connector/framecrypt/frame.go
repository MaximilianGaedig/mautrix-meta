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
	"sort"
	"strings"
)

// ErrEncryptionOff is returned by Encrypt when the last server update
// turned E2EE off and the keys manager is InfraMandated: the web client
// drops such frames (ZenonSecureFrameManager.encrypt returns an empty
// buffer).
var ErrEncryptionOff = errors.New("framecrypt: E2EE negotiated off and mandated, frame dropped")

// FrameCryptError is a failed Encrypt or Decrypt.
//
// The module's frame APIs do not return SecureFrameErrors codes, whatever
// the glue's use of the SecureFrameErrors table suggests: the per-handler
// decrypt wrapper (func 3416 in the shipped build) turns every inner error
// except "missing key" into status 1 and "missing key" into 2, and
// frameEncryptor_encrypt returns just 0 or 1. The web client therefore
// logs "wasm_decryption_error_alloc" / "..._invalidparam" for what are
// really authentication failures and missing keys. The inner error is only
// recorded in the keys manager's E2EE metrics (GroupE2eeMetrics
// decryption_error_frames_* / encryption_error_frames_* counters), so
// Reason is recovered by diffing those counters around the call. Calls are
// serialised per instance and successful frames do not touch these
// counters, so the attribution is exact.
type FrameCryptError struct {
	Op     string     // "encrypt" or "decrypt"
	Status int32      // the export's return value
	Reason FrameError // the SecureFrame error, 0 if no counter identified it
	// Counters names the metrics counters that moved, e.g.
	// "decryption_error_frames_cipher_auth".
	Counters []string
}

func (e *FrameCryptError) Error() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "framecrypt: %s failed (status %d", e.Op, e.Status)
	if e.Reason != 0 {
		sb.WriteString(", " + frameErrorNames[e.Reason])
	}
	if len(e.Counters) > 0 {
		sb.WriteString("; " + strings.Join(e.Counters, ", "))
	}
	sb.WriteString(")")
	return sb.String()
}

// Unwrap returns Reason, so errors.Is(err, ErrCipherAuth) works.
func (e *FrameCryptError) Unwrap() error {
	if e.Reason == 0 {
		return nil
	}
	return e.Reason
}

type frameCounter struct {
	name   string
	reason FrameError
}

// frameErrorCounters are the GroupE2eeMetrics fields (field id → name, as
// in the web client's E2eeMetricsSerializers) counting per-frame errors
// of the signalling-path frame encryptor/decryptor.
var frameErrorCounters = map[int16]frameCounter{
	60:  {"decryption_error_frames_alloc", ErrAlloc},
	61:  {"decryption_error_frames_invalid_params", ErrInvalidParam},
	62:  {"decryption_error_frames_cipher", ErrCipher},
	63:  {"decryption_error_frames_parse", ErrParse},
	64:  {"decryption_error_frames_invalid_key", ErrInvalidKey},
	65:  {"decryption_error_frames_missing_key", ErrMissingKey},
	66:  {"decryption_error_frames_out_of_ratchet_space", ErrOutOfRatchetSpace},
	67:  {"decryption_error_frames_cipher_auth", ErrCipherAuth},
	68:  {"decryption_error_frames_frame_too_old", ErrFrameTooOld},
	69:  {"decryption_error_frames_seen_frame", ErrSeenFrame},
	70:  {"decryption_error_frames_invalid_frame", ErrInvalidFrame},
	71:  {"decryption_error_frames_setting_invalid_key", ErrSettingInvalidKey},
	72:  {"decryption_error_frames_setting_existing_key", ErrSettingExistingKey},
	73:  {"decryption_error_frames_escape_data", ErrEscapeData},
	74:  {"decryption_error_frames_deescape_data", ErrDeescapeData},
	75:  {"decryption_error_frames_parse_frame_or_key", ErrParseFrameOrKey},
	76:  {"decryption_error_frames_unknown", 0},
	82:  {"encryption_error_frames_alloc", ErrAlloc},
	83:  {"encryption_error_frames_invalid_params", ErrInvalidParam},
	84:  {"encryption_error_frames_cipher", ErrCipher},
	85:  {"encryption_error_frames_parse", ErrParse},
	86:  {"encryption_error_frames_invalid_key", ErrInvalidKey},
	87:  {"encryption_error_frames_cipher_auth", ErrCipherAuth},
	88:  {"encryption_error_frames_escape_data", ErrEscapeData},
	89:  {"encryption_error_frames_unsupported_codec", ErrUnsupportedCodec},
	90:  {"encryption_error_frames_unknown", 0},
	146: {"g_e2ee_encryption_error_frames_empty", 0},
	147: {"g_e2ee_encryption_error_frames_empty_nalu_blocks", 0},
	148: {"g_e2ee_encryption_error_frames_invalid_h264", 0},
	149: {"g_e2ee_encryption_error_frames_invalid_h265", 0},
	150: {"g_e2ee_encryption_error_frames_invalid_h265_nalu_block", 0},
	153: {"encryption_error_frames_no_active_key", ErrMissingKey},
}

// snapshotErrors records the current error counters. Caller holds the lock.
func (km *KeysManager) snapshotErrors(ctx context.Context) error {
	m, err := km.metrics(ctx)
	if err != nil {
		return err
	}
	all, err := groupMetricInts(m)
	if err != nil {
		return err
	}
	km.errCounters = map[int16]int64{}
	for id := range frameErrorCounters {
		km.errCounters[id] = all[id]
	}
	return nil
}

// frameError builds the error for a failed frame call by diffing the
// error counters against the last snapshot. Caller holds the lock.
func (km *KeysManager) frameError(ctx context.Context, op string, status uint32) error {
	fe := &FrameCryptError{Op: op, Status: int32(status)}
	prev := km.errCounters
	if prev == nil {
		// No baseline: only a counter that is exactly 1 is attributable.
		prev = map[int16]int64{}
	}
	if err := km.snapshotErrors(ctx); err != nil {
		return errors.Join(fe, err)
	}
	var ids []int
	for id, v := range km.errCounters {
		if v > prev[id] {
			ids = append(ids, int(id))
		}
	}
	sort.Ints(ids)
	for _, id := range ids {
		c := frameErrorCounters[int16(id)]
		fe.Counters = append(fe.Counters, c.name)
		if fe.Reason == 0 && c.reason != 0 {
			fe.Reason = c.reason
		}
	}
	return fe
}

// scratch returns a heap buffer of at least n bytes owned by the instance,
// reused across frame calls (they are serialised) so a frame costs no
// malloc/free. Caller holds the lock.
func (in *Instance) scratch(ctx context.Context, idx int, n uint32) (uint32, error) {
	s := &in.scratchBufs[idx]
	if s.size >= n && s.ptr != 0 {
		return s.ptr, nil
	}
	size := max(n, 4096)
	size += size / 4
	p, err := in.call32(ctx, "malloc", uint64(size))
	if err != nil {
		return 0, err
	}
	if p == 0 {
		return 0, errors.New("framecrypt: malloc failed")
	}
	if s.ptr != 0 {
		in.free(ctx, s.ptr)
	}
	s.ptr, s.size = p, size
	return p, nil
}

// FrameEncryptor encrypts outgoing frames of one track.
type FrameEncryptor struct {
	km  *KeysManager
	ptr uint32
}

// NewFrameEncryptor creates a frameEncryptor (FrameEncryptionWasm
// createFrameEncryptor).
func (km *KeysManager) NewFrameEncryptor(ctx context.Context) (*FrameEncryptor, error) {
	in := km.in
	fe := &FrameEncryptor{km: km}
	err := in.do(ctx, func(ctx context.Context) error {
		deps, err := in.call32(ctx, "frameEncryptorDeps_create")
		if err != nil {
			return err
		}
		defer func() { _, _ = in.call(ctx, "frameEncryptorDeps_free", uint64(deps)) }()
		if _, err = in.call(ctx, "frameEncryptorDeps_setEncryptionKeysManager", uint64(deps), uint64(km.ptr)); err != nil {
			return err
		}
		if _, err = in.call(ctx, "frameEncryptorDeps_setLoggingCallback", uint64(deps), uint64(in.logSlot)); err != nil {
			return err
		}
		fe.ptr, err = in.call32(ctx, "frameEncryptor_create", uint64(deps))
		if err == nil && fe.ptr == 0 {
			err = errors.New("framecrypt: frameEncryptor_create returned null")
		}
		if err == nil && km.errCounters == nil {
			err = km.snapshotErrors(ctx)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return fe, nil
}

// Encrypt encrypts one encoded frame (the payload an insertable-streams
// transform sees: a whole Opus packet, a whole VP8 frame or H264 access
// unit in Annex B form), as ZenonSecureFrameManager.encrypt does: empty
// frames and, while E2EE is negotiated off, all frames are returned
// unencrypted (or ErrEncryptionOff when InfraMandated). With HandlerH264
// the module encrypts the NAL unit payloads and leaves the start codes and
// NAL headers in the clear; frames that don't parse as H264 fall back to
// Generic inside the module. A failure is a *FrameCryptError; the web
// client drops such frames when E2EE is mandated.
func (fe *FrameEncryptor) Encrypt(ctx context.Context, handler FrameDataHandlerType, frame []byte) ([]byte, error) {
	if len(frame) == 0 {
		return frame, nil
	}
	km := fe.km
	in := km.in
	var out []byte
	err := in.do(ctx, func(ctx context.Context) error {
		if fe.ptr == 0 {
			return errors.New("framecrypt: encryptor is closed")
		}
		if km.encDisabled {
			if km.infraMandated {
				return ErrEncryptionOff
			}
			out = append([]byte(nil), frame...)
			return nil
		}
		// v(): a buffer of getMaxEncryptedSize bytes holding the frame
		// followed by zeros; encryption happens in place.
		size, err := in.call32(ctx, "frameEncryptor_getMaxEncryptedSize", uint64(fe.ptr), uint64(len(frame)))
		if err != nil {
			return err
		}
		if int(size) < len(frame) {
			return fmt.Errorf("framecrypt: max encrypted size %d < frame size %d", size, len(frame))
		}
		bp, err := in.scratch(ctx, 0, size)
		if err != nil {
			return err
		}
		lp, err := in.scratch(ctx, 2, 4)
		if err != nil {
			return err
		}
		mem := in.mod.Memory()
		if !mem.Write(bp, frame) || !mem.Write(bp+uint32(len(frame)), make([]byte, int(size)-len(frame))) || !mem.WriteUint32Le(lp, 0) {
			return errors.New("framecrypt: write out of bounds")
		}
		status, err := in.call32(ctx, "frameEncryptor_encrypt", uint64(fe.ptr), uint64(uint32(handler)),
			uint64(bp), uint64(len(frame)), uint64(bp), uint64(size), 0, 0, uint64(lp))
		if err != nil {
			return err
		}
		if status != 0 {
			return km.frameError(ctx, "encrypt", status)
		}
		n := getU32(in.mod, lp)
		if n > size {
			return fmt.Errorf("framecrypt: encrypt reported %d bytes in a %d byte buffer", n, size)
		}
		out = in.read(bp, n)
		return nil
	})
	return out, err
}

// Close frees the encryptor.
func (fe *FrameEncryptor) Close(ctx context.Context) error {
	return fe.km.in.do(ctx, func(ctx context.Context) error {
		if fe.ptr == 0 {
			return nil
		}
		_, err := fe.km.in.call(ctx, "frameEncryptor_free", uint64(fe.ptr))
		fe.ptr = 0
		return err
	})
}

// FrameDecryptor decrypts incoming frames of one remote track.
type FrameDecryptor struct {
	km       *KeysManager
	ptr      uint32
	e2eeID   string
	handlers []FrameDataHandlerType
}

// NewFrameDecryptor creates a frameDecryptor for frames sent by the
// participant with the given E2EE id ("<userId>:<cname>", the senderId of
// its E2eeKey messages), accepting the given handler types (see
// DecryptorHandlers; ZenonSecureFrameManager.createDecryptorWithId sets
// them right away). While E2EE is negotiated off the decryptor also
// accepts unencrypted frames, as the web client's allowUnencryptedFrames.
func (km *KeysManager) NewFrameDecryptor(ctx context.Context, e2eeID string, handlers []FrameDataHandlerType) (*FrameDecryptor, error) {
	fd := &FrameDecryptor{km: km, e2eeID: e2eeID, handlers: append([]FrameDataHandlerType(nil), handlers...)}
	err := km.in.do(ctx, func(ctx context.Context) error {
		if err := fd.create(ctx); err != nil {
			return err
		}
		km.decryptors[fd] = struct{}{}
		if km.errCounters == nil {
			return km.snapshotErrors(ctx)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fd, nil
}

// create makes the module object. Caller holds the lock.
func (fd *FrameDecryptor) create(ctx context.Context) error {
	km := fd.km
	in := km.in
	deps, err := in.call32(ctx, "frameDecryptorDeps_create")
	if err != nil {
		return err
	}
	defer func() { _, _ = in.call(ctx, "frameDecryptorDeps_free", uint64(deps)) }()
	if _, err = in.call(ctx, "frameDecryptorDeps_setEncryptionKeysManager", uint64(deps), uint64(km.ptr)); err != nil {
		return err
	}
	err = in.withBytes(ctx, []byte(fd.e2eeID), func(p, n uint32) error {
		_, err := in.call(ctx, "frameDecryptorDeps_setE2eeId", uint64(deps), uint64(p), uint64(n))
		return err
	})
	if err != nil {
		return err
	}
	if _, err = in.call(ctx, "frameDecryptorDeps_setLoggingCallback", uint64(deps), uint64(in.logSlot)); err != nil {
		return err
	}
	ptr, err := in.call32(ctx, "frameDecryptor_create", uint64(deps))
	if err != nil {
		return err
	}
	if ptr == 0 {
		return errors.New("framecrypt: frameDecryptor_create returned null")
	}
	fd.ptr = ptr
	if err = fd.setHandlers(ctx, fd.handlers); err != nil {
		return err
	}
	if km.encDisabled {
		_, err = in.call(ctx, "frameDecryptor_enableUnencryptedData", uint64(fd.ptr))
	}
	return err
}

// recreate replaces the module object (see applyNegotiation). Caller
// holds the lock.
func (fd *FrameDecryptor) recreate(ctx context.Context) error {
	if fd.ptr != 0 {
		if _, err := fd.km.in.call(ctx, "frameDecryptor_free", uint64(fd.ptr)); err != nil {
			return err
		}
		fd.ptr = 0
	}
	return fd.create(ctx)
}

// SetSupportedFrameDataHandlerTypes changes the handler types the
// decryptor accepts (ZenonSecureFrameManager.setFrameDataHandlerTypesOnDecryptor
// once the codec is negotiated; the web client never changes audio
// decryptors).
func (fd *FrameDecryptor) SetSupportedFrameDataHandlerTypes(ctx context.Context, types []FrameDataHandlerType) error {
	return fd.km.in.do(ctx, func(ctx context.Context) error {
		fd.handlers = append([]FrameDataHandlerType(nil), types...)
		return fd.setHandlers(ctx, types)
	})
}

func (fd *FrameDecryptor) setHandlers(ctx context.Context, types []FrameDataHandlerType) error {
	in := fd.km.in
	buf := make([]byte, 4*len(types))
	for i, t := range types {
		v := uint32(t)
		buf[4*i], buf[4*i+1], buf[4*i+2], buf[4*i+3] = byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
	}
	// setSupportedFrameDataHandlerTypes traps on a null array, so pass a
	// real (possibly empty) allocation as the glue's malloc(0) does.
	return in.withBytes(ctx, buf, func(p, _ uint32) error {
		_, err := in.call(ctx, "frameDecryptor_setSupportedFrameDataHandlerTypes", uint64(fd.ptr), uint64(p), uint64(len(types)))
		return err
	})
}

// EnableUnencryptedData lets the decryptor pass unencrypted frames through
// (the glue's allowUnencryptedFrames). Decryptors of a keys manager whose
// server update turned E2EE off get this automatically.
func (fd *FrameDecryptor) EnableUnencryptedData(ctx context.Context) error {
	return fd.km.in.do(ctx, func(ctx context.Context) error {
		_, err := fd.km.in.call(ctx, "frameDecryptor_enableUnencryptedData", uint64(fd.ptr))
		return err
	})
}

// Decrypt decrypts one received frame, as ZenonSecureFrameManager.decrypt:
// empty frames, and all frames once decryption was disabled
// (removeFrameDecryptorDelayMs after E2EE was negotiated off), are
// returned as they are. A failure is a *FrameCryptError; errors.Is works
// with its Reason (ErrCipherAuth for a tampered frame, ErrMissingKey when
// no key from the sender is known).
func (fd *FrameDecryptor) Decrypt(ctx context.Context, frame []byte) ([]byte, error) {
	if len(frame) == 0 {
		return frame, nil
	}
	km := fd.km
	in := km.in
	var out []byte
	err := in.do(ctx, func(ctx context.Context) error {
		if fd.ptr == 0 {
			return errors.New("framecrypt: decryptor is closed")
		}
		if km.decDisabled {
			out = append([]byte(nil), frame...)
			return nil
		}
		n := uint32(len(frame))
		// Input and output must be distinct buffers (frameDecryptor_decrypt
		// hits unreachable otherwise); the output is never longer than the
		// input.
		ip, err := in.scratch(ctx, 0, n)
		if err != nil {
			return err
		}
		op, err := in.scratch(ctx, 1, n)
		if err != nil {
			return err
		}
		lp, err := in.scratch(ctx, 2, 4)
		if err != nil {
			return err
		}
		if !in.mod.Memory().Write(ip, frame) || !in.mod.Memory().WriteUint32Le(lp, 0) {
			return errors.New("framecrypt: write out of bounds")
		}
		status, err := in.call32(ctx, "frameDecryptor_decrypt", uint64(fd.ptr), uint64(ip), uint64(n),
			uint64(op), uint64(n), 0, 0, uint64(lp))
		if err != nil {
			return err
		}
		if status != 0 {
			return km.frameError(ctx, "decrypt", status)
		}
		l := getU32(in.mod, lp)
		if l > n {
			return fmt.Errorf("framecrypt: decrypt reported %d bytes in a %d byte buffer", l, n)
		}
		out = in.read(op, l)
		return nil
	})
	return out, err
}

// Close frees the decryptor.
func (fd *FrameDecryptor) Close(ctx context.Context) error {
	return fd.km.in.do(ctx, func(ctx context.Context) error {
		if fd.ptr == 0 {
			return nil
		}
		delete(fd.km.decryptors, fd)
		_, err := fd.km.in.call(ctx, "frameDecryptor_free", uint64(fd.ptr))
		fd.ptr = 0
		return err
	})
}
