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
	"sync"
	"sync/atomic"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// LogFunc receives the module's log output: level is "ERROR", "WARNING",
// "INFO" for the module's logging callback (mapped with the module's own
// ConsoleLogLevelDefinition, as FrameEncryptionWasm's R() does) or
// "STDOUT"/"STDERR" for text it printed through fd_write.
type LogFunc func(level, msg string)

// Options configures a Runtime.
type Options struct {
	// Log receives module log lines. It is called after the API call that
	// produced them returns, never while the instance is locked.
	Log LogFunc
}

// callbackPool lists the emscripten addFunction signatures the glue uses
// (FrameEncryptionWasm: "viii" log, "vi" e2eeContext model update and
// stopTimer, "i" getLocalPublicPrivateIdentityKey and getLocalDeviceId,
// "iiii" getRemotePublicIdentityKey, "iiiiii" saveRemotePublicIdentityKey,
// "viiii" send(Sctp)E2eeMessage, "iii" startTimer) and how many table slots
// each gets per instance.
var callbackPool = []struct {
	sig   callbackSig
	count int
}{
	{"viii", 4},
	{"vi", 32},
	{"i", 32},
	{"iiii", 16},
	{"iiiiii", 16},
	{"viiii", 32},
	{"iii", 16},
}

func poolSigs() []callbackSig {
	var out []callbackSig
	for _, p := range callbackPool {
		for i := 0; i < p.count; i++ {
			out = append(out, p.sig)
		}
	}
	return out
}

// requiredExports are the exports this package calls.
var requiredExports = []string{
	"__wasm_call_ctors", "malloc", "free", "logSeverityDefinition_create",
	"byteBuffer_getDataPtr", "byteBuffer_getSize", "byteBuffer_free",
	"proxyIdentityStoreDeps_create", "proxyIdentityStoreDeps_free",
	"proxyIdentityStoreDeps_setGetLocalPublicPrivateIdentityKeyCallback",
	"proxyIdentityStoreDeps_setGetRemotePublicIdentityKeyCallback",
	"proxyIdentityStoreDeps_setSaveRemotePublicIdentityKeyCallback",
	"proxyIdentityStoreDeps_setGetLocalDeviceIdCallback",
	"proxyIdentityStoreDeps_setLocalUserId", "proxyIdentityStoreDeps_setLocalIdentityKeyMode",
	"identityKey_create", "publicKey_create", "e2eeContext_create", "e2eeContext_free",
	"encryptionKeysManagerDeps_create", "encryptionKeysManagerDeps_free",
	"encryptionKeysManagerDeps_setE2eeContext", "encryptionKeysManagerDeps_setSendE2eeMessageCallback",
	"encryptionKeysManagerDeps_setSctpSendE2eeMessageCallback", "encryptionKeysManagerDeps_setUserIdStr",
	"encryptionKeysManagerDeps_setStartTimerCallback", "encryptionKeysManagerDeps_setStopTimerCallback",
	"encryptionKeysManagerDeps_setIsSessionKeyEnabled", "encryptionKeysManagerDeps_setIsE2eeInfraMandated",
	"encryptionKeysManager_create", "encryptionKeysManager_free", "encryptionKeysManager_setLocalE2eeId",
	"encryptionKeysManager_getSerializedE2eeClientState", "encryptionKeysManager_processE2eeServerUpdate",
	"encryptionKeysManager_processE2eeMessage", "encryptionKeysManager_updateSenderKeyIndex",
	"encryptionKeysManager_onMediaDataChannelReady", "wasmTimerAdapter_execute",
	"frameEncryptorDeps_create", "frameEncryptorDeps_free", "frameEncryptorDeps_setEncryptionKeysManager",
	"frameEncryptorDeps_setLoggingCallback", "frameEncryptor_create", "frameEncryptor_free",
	"frameEncryptor_encrypt", "frameEncryptor_getMaxEncryptedSize",
	"frameDecryptorDeps_create", "frameDecryptorDeps_free", "frameDecryptorDeps_setEncryptionKeysManager",
	"frameDecryptorDeps_setE2eeId", "frameDecryptorDeps_setLoggingCallback", "frameDecryptor_create",
	"frameDecryptor_free", "frameDecryptor_decrypt", "frameDecryptor_setSupportedFrameDataHandlerTypes",
	"frameDecryptor_enableUnencryptedData", "dataResult_getErrCode", "dataResult_getData",
	"dataResult_getDataSize", "dataResult_free", "stringBuffer_getDataPtr", "stringBuffer_getSize",
	"stringBuffer_free",
	"encryptionKeysManagerProcessE2eeServerUpdateResult_getErrCode",
	"encryptionKeysManagerProcessE2eeServerUpdateResult_getKeyIndexDataPtr",
	"encryptionKeysManagerProcessE2eeServerUpdateResult_getKeyIndexSize",
	"encryptionKeysManagerProcessE2eeServerUpdateResult_getKeyUpdateDelay",
	"encryptionKeysManagerProcessE2eeServerUpdateResult_getRemoveFrameDecryptorDelayMs",
	"encryptionKeysManagerProcessE2eeServerUpdateResult_free",
	"encryptionKeysManagerProcessE2eeMessageResult_getStatusCode",
	"encryptionKeysManagerProcessE2eeMessageResult_getKeyIndexDataPtr",
	"encryptionKeysManagerProcessE2eeMessageResult_getKeyIndexSize",
	"encryptionKeysManagerProcessE2eeMessageResult_getKeyUpdateDelay",
	"encryptionKeysManagerProcessE2eeMessageResult_free",
}

// Runtime holds the compiled module. Create one per process and any number
// of instances from it.
type Runtime struct {
	r        wazero.Runtime
	compiled wazero.CompiledModule
	info     *RewriteInfo
	sigs     []callbackSig
	opts     Options

	seq       atomic.Uint64
	instances sync.Map // module name → *Instance
}

// NewRuntime rewrites (see rewriteModule) and compiles the frame-encryption
// module. wasm is the file the web client downloads (see the package doc);
// it is not bundled.
func NewRuntime(ctx context.Context, wasm []byte, opts *Options) (*Runtime, error) {
	rt := &Runtime{sigs: poolSigs()}
	if opts != nil {
		rt.opts = *opts
	}
	rewritten, info, err := rewriteModule(wasm, uint32(len(rt.sigs)))
	if err != nil {
		return nil, fmt.Errorf("framecrypt: rewrite: %w", err)
	}
	rt.info = info
	rt.r = wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().WithCloseOnContextDone(false))
	if err = instantiateHost(ctx, rt.r, "env", rt.envFunctions()); err != nil {
		rt.r.Close(ctx)
		return nil, fmt.Errorf("framecrypt: env: %w", err)
	}
	if err = instantiateHost(ctx, rt.r, "wasi_snapshot_preview1", rt.wasiFunctions()); err != nil {
		rt.r.Close(ctx)
		return nil, fmt.Errorf("framecrypt: wasi: %w", err)
	}
	rt.compiled, err = rt.r.CompileModule(ctx, rewritten)
	if err != nil {
		rt.r.Close(ctx)
		return nil, fmt.Errorf("framecrypt: compile: %w", err)
	}
	exports := rt.compiled.ExportedFunctions()
	var missing []string
	for _, name := range requiredExports {
		if _, ok := exports[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		rt.r.Close(ctx)
		return nil, fmt.Errorf("framecrypt: not the frame_encryption module (missing exports %s)", strings.Join(missing, ", "))
	}
	return rt, nil
}

// Close releases the runtime and all its instances.
func (rt *Runtime) Close(ctx context.Context) error { return rt.r.Close(ctx) }

func (rt *Runtime) instanceOf(mod api.Module) *Instance {
	if v, ok := rt.instances.Load(mod.Name()); ok {
		return v.(*Instance)
	}
	return nil
}

func (rt *Runtime) hostMalloc(ctx context.Context, mod api.Module, data []byte) uint32 {
	res, err := mod.ExportedFunction("malloc").Call(ctx, uint64(len(data)))
	if err != nil {
		panic(err)
	}
	p := uint32(res[0])
	if p == 0 || !mod.Memory().Write(p, data) {
		panic(&abortError{"host malloc failed"})
	}
	return p
}

func (rt *Runtime) stdio(_ context.Context, mod api.Module, fd uint32, data []byte) {
	in := rt.instanceOf(mod)
	if in == nil {
		return
	}
	level := "STDOUT"
	if fd == 2 {
		level = "STDERR"
	}
	for _, line := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		in.deferLog(level, line)
	}
}

// Instance is one instantiation of the module: its own linear memory, heap
// and objects, as one browser tab has. It and everything created from it
// may be used from any number of goroutines (e.g. one per media track):
// all calls are serialised by one mutex, like the browser's single JS
// thread, and key-index and module timers take the same mutex when they
// fire. Callbacks from the module (identity store lookups) run
// synchronously on the calling goroutine while it is held and must not
// call back into the Instance. Outbound events (SendE2eeMessage, model
// updates, logs) are queued and delivered after the call returns, outside
// the lock.
type Instance struct {
	rt   *Runtime
	name string
	mod  api.Module
	host api.Module
	shim api.Module

	mu      sync.Mutex
	fns     map[string]api.Function
	slots   []slotFunc
	aborted error
	closed  bool
	pending []func()

	logSlot   uint32
	logLevels map[int32]string

	timerSeq     int32
	timers       map[int32]*timerEntry
	timersFired  int
	timersCancel int

	fds map[int32]*randomDevice

	// scratchBufs are reusable heap buffers for frame data (see scratch).
	scratchBufs [3]struct{ ptr, size uint32 }
}

type slotFunc func(ctx context.Context, st []uint64)

// ErrClosed is returned by calls on a closed instance.
var ErrClosed = errors.New("framecrypt: instance closed")

// NewInstance instantiates the module and runs its constructors, as the
// glue's run() does (the start section zeroes .bss; __wasm_call_ctors
// initialises the stack limits, environment and static constructors).
func (rt *Runtime) NewInstance(ctx context.Context) (*Instance, error) {
	n := rt.seq.Add(1)
	in := &Instance{
		rt:     rt,
		name:   fmt.Sprintf("frame_encryption-%d", n),
		fns:    map[string]api.Function{},
		slots:  make([]slotFunc, len(rt.sigs)),
		timers: map[int32]*timerEntry{},
		fds:    map[int32]*randomDevice{},
	}
	rt.instances.Store(in.name, in)
	var err error
	in.mod, err = rt.r.InstantiateModule(ctx, rt.compiled, wazero.NewModuleConfig().WithName(in.name).WithStartFunctions())
	if err != nil {
		rt.instances.Delete(in.name)
		return nil, fmt.Errorf("framecrypt: instantiate: %w", err)
	}
	hostName := in.name + "-callbacks"
	hb := rt.r.NewHostModuleBuilder(hostName)
	for i, sig := range rt.sigs {
		params := make([]api.ValueType, sig.params())
		for j := range params {
			params[j] = i32
		}
		var results []api.ValueType
		if sig.hasResult() {
			results = []api.ValueType{i32}
		}
		idx := i
		hb.NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, _ api.Module, st []uint64) {
			in.dispatch(ctx, idx, st)
		}), params, results).Export(fmt.Sprintf("s%d", i))
	}
	if in.host, err = hb.Instantiate(ctx); err != nil {
		in.closeModules(ctx)
		return nil, fmt.Errorf("framecrypt: callbacks: %w", err)
	}
	shim := buildShim(hostName, in.name, rt.sigs, rt.info.TableSize, rt.info.TableBase)
	if in.shim, err = rt.r.InstantiateWithConfig(ctx, shim, wazero.NewModuleConfig().WithName(in.name+"-shim").WithStartFunctions()); err != nil {
		in.closeModules(ctx)
		return nil, fmt.Errorf("framecrypt: callback shim: %w", err)
	}
	err = in.do(ctx, func(ctx context.Context) error {
		if _, err := in.call(ctx, "__wasm_call_ctors"); err != nil {
			return err
		}
		if err := in.loadLogLevels(ctx); err != nil {
			return err
		}
		var err error
		in.logSlot, err = in.allocSlot("viii", func(ctx context.Context, st []uint64) {
			level := in.logLevels[int32(st[0])]
			if level == "" {
				level = "INFO"
			}
			in.deferLog(level, string(readBytes(in.mod, uint32(st[1]), uint32(st[2]))))
		})
		return err
	})
	if err != nil {
		in.closeModules(ctx)
		return nil, err
	}
	return in, nil
}

// Close stops timers and releases the instance's modules.
func (in *Instance) Close(ctx context.Context) error {
	in.mu.Lock()
	in.closed = true
	for id, t := range in.timers {
		t.t.Stop()
		delete(in.timers, id)
	}
	in.mu.Unlock()
	return in.closeModules(ctx)
}

func (in *Instance) closeModules(ctx context.Context) error {
	in.rt.instances.Delete(in.name)
	var errs []error
	for _, m := range []api.Module{in.shim, in.host, in.mod} {
		if m != nil {
			errs = append(errs, m.Close(ctx))
		}
	}
	return errors.Join(errs...)
}

// do runs f with the instance locked, then delivers queued events.
func (in *Instance) do(ctx context.Context, f func(ctx context.Context) error) error {
	in.mu.Lock()
	if in.closed {
		in.mu.Unlock()
		return ErrClosed
	}
	if in.aborted != nil {
		in.mu.Unlock()
		return in.aborted
	}
	err := f(ctx)
	pending := in.pending
	in.pending = nil
	in.mu.Unlock()
	for _, p := range pending {
		p()
	}
	return err
}

func (in *Instance) deferLog(level, msg string) {
	if in.rt.opts.Log != nil {
		log := in.rt.opts.Log
		in.pending = append(in.pending, func() { log(level, msg) })
	}
}

func (in *Instance) deferEvent(f func()) { in.pending = append(in.pending, f) }

// call invokes an export. Callers hold the lock. A trap (including abort()
// and C++ throws) poisons the instance, as it leaves the module's stack
// and heap in an unknown state; the glue likewise reports it through
// terminateCallFn and stops using the module.
func (in *Instance) call(ctx context.Context, name string, args ...uint64) (uint64, error) {
	fn := in.fns[name]
	if fn == nil {
		fn = in.mod.ExportedFunction(name)
		if fn == nil {
			return 0, fmt.Errorf("framecrypt: module has no export %q", name)
		}
		in.fns[name] = fn
	}
	// JS calls exports with whatever arguments the glue passes: extra ones
	// are dropped and missing ones become undefined → 0. The glue relies on
	// both (e.g. a log callback passed to the one-parameter
	// setGetLocalPublicPrivateIdentityKeyCallback, three arguments to the
	// four-parameter processE2eeMessage), so do the same.
	if want := len(fn.Definition().ParamTypes()); len(args) != want {
		fixed := make([]uint64, want)
		copy(fixed, args)
		args = fixed
	}
	res, err := fn.Call(ctx, args...)
	if err != nil {
		in.aborted = fmt.Errorf("framecrypt: %s: %w", name, err)
		return 0, in.aborted
	}
	if len(res) == 0 {
		return 0, nil
	}
	return res[0], nil
}

func (in *Instance) call32(ctx context.Context, name string, args ...uint64) (uint32, error) {
	v, err := in.call(ctx, name, args...)
	return uint32(v), err
}

// mustCall is call for use inside callbacks, where an error must unwind
// the whole wasm call.
func (in *Instance) mustCall(ctx context.Context, name string, args ...uint64) uint32 {
	v, err := in.call32(ctx, name, args...)
	if err != nil {
		panic(err)
	}
	return v
}

// malloc copies data into a fresh heap buffer (allocateWasmBuffer).
func (in *Instance) malloc(ctx context.Context, data []byte) (uint32, error) {
	p, err := in.call32(ctx, "malloc", uint64(len(data)))
	if err != nil {
		return 0, err
	}
	if p == 0 && len(data) > 0 {
		return 0, errors.New("framecrypt: malloc failed")
	}
	if len(data) > 0 && !in.mod.Memory().Write(p, data) {
		return 0, errors.New("framecrypt: write out of bounds")
	}
	return p, nil
}

func (in *Instance) free(ctx context.Context, ptrs ...uint32) {
	for _, p := range ptrs {
		if p != 0 {
			_, _ = in.call(ctx, "free", uint64(p))
		}
	}
}

func (in *Instance) read(ptr, n uint32) []byte { return readBytes(in.mod, ptr, n) }

// withString mallocs s (allocateWasmBufferFromString: one byte per UTF-16
// unit, i.e. latin-1, which for the ASCII ids used here is the bytes),
// calls f with pointer and length, and frees it.
func (in *Instance) withBytes(ctx context.Context, b []byte, f func(ptr, n uint32) error) error {
	p, err := in.malloc(ctx, b)
	if err != nil {
		return err
	}
	defer in.free(ctx, p)
	return f(p, uint32(len(b)))
}

func (in *Instance) allocSlot(sig callbackSig, f slotFunc) (uint32, error) {
	for i, s := range in.rt.sigs {
		if s == sig && in.slots[i] == nil {
			in.slots[i] = f
			return in.rt.info.TableBase + uint32(i), nil
		}
	}
	return 0, fmt.Errorf("framecrypt: no free %q callback slot", sig)
}

func (in *Instance) freeSlots(idx ...uint32) {
	for _, t := range idx {
		if t >= in.rt.info.TableBase {
			in.slots[t-in.rt.info.TableBase] = nil
		}
	}
}

func (in *Instance) dispatch(ctx context.Context, idx int, st []uint64) {
	f := in.slots[idx]
	if f == nil {
		panic(&abortError{fmt.Sprintf("call to unassigned callback slot %d (%s)", idx, in.rt.sigs[idx])})
	}
	f(ctx, st)
}

// loadLogLevels reads the module's ConsoleLogLevelDefinition (Thrift
// compact {1 error byte, 2 warning byte, 3 info byte}), like the glue's S().
func (in *Instance) loadLogLevels(ctx context.Context) error {
	bb, err := in.call32(ctx, "logSeverityDefinition_create")
	if err != nil {
		return err
	}
	data, err := in.byteBuffer(ctx, bb)
	if err != nil {
		return err
	}
	_, _ = in.call(ctx, "byteBuffer_free", uint64(bb))
	in.logLevels = map[int32]string{}
	names := map[int16]string{1: "ERROR", 2: "WARNING", 3: "INFO"}
	last := int16(0)
	for p := 0; p+1 < len(data) && data[p] != 0; {
		h := data[p]
		p++
		if h>>4 == 0 || h&0xf != 3 {
			break
		}
		last += int16(h >> 4)
		// Insertion order matters when levels collide; the glue's switch
		// checks error first, then warning, then info.
		if _, dup := in.logLevels[int32(int8(data[p]))]; !dup {
			in.logLevels[int32(int8(data[p]))] = names[last]
		}
		p++
	}
	return nil
}

func (in *Instance) byteBuffer(ctx context.Context, bb uint32) ([]byte, error) {
	ptr, err := in.call32(ctx, "byteBuffer_getDataPtr", uint64(bb))
	if err != nil {
		return nil, err
	}
	n, err := in.call32(ctx, "byteBuffer_getSize", uint64(bb))
	if err != nil {
		return nil, err
	}
	return in.read(ptr, n), nil
}
