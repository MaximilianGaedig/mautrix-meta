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
	"crypto/rand"
	"fmt"
	"strings"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// The module's imports, with the minimal behaviour a headless host needs.
// The reference is the emscripten glue shipped as the "frame_encryption"
// module (wasmImports in its factory): embind registrations only feed JS
// type wrappers the web client never uses (it calls the C exports
// directly), the filesystem is emscripten's in-memory FS that only serves
// stdout/stderr, and time comes from Date/performance.

// abortError is raised (as a panic, which wazero turns into a call error)
// by abort() and __cxa_throw. The module is built without exception
// handling support (no invoke_* imports), so a C++ throw can never be
// caught inside and always ends the call, as it does in the browser.
type abortError struct{ msg string }

func (e *abortError) Error() string { return "framecrypt: wasm aborted: " + e.msg }

const (
	i32 = api.ValueTypeI32
	i64 = api.ValueTypeI64
	f64 = api.ValueTypeF64
)

// Emscripten errno values (system/lib/libc/musl/arch/emscripten/bits/errno.h).
const (
	errnoBADF   = 8
	errnoINVAL  = 28
	errnoNOENT  = 44
	errnoNOSYS  = 52
	errnoSPIPE  = 70
	errnoNOTTY  = 59
	wasiSuccess = 0
)

type hostFn struct {
	name    string
	params  []api.ValueType
	results []api.ValueType
	fn      api.GoModuleFunc
}

func memOf(mod api.Module) api.Memory { return mod.Memory() }

func putU32(mod api.Module, ptr, v uint32) {
	if !memOf(mod).WriteUint32Le(ptr, v) {
		panic(&abortError{fmt.Sprintf("host write out of bounds at %#x", ptr)})
	}
}

func getU32(mod api.Module, ptr uint32) uint32 {
	v, ok := memOf(mod).ReadUint32Le(ptr)
	if !ok {
		panic(&abortError{fmt.Sprintf("host read out of bounds at %#x", ptr)})
	}
	return v
}

func readBytes(mod api.Module, ptr, n uint32) []byte {
	if n == 0 {
		return []byte{}
	}
	b, ok := memOf(mod).Read(ptr, n)
	if !ok {
		panic(&abortError{fmt.Sprintf("host read out of bounds at %#x+%d", ptr, n)})
	}
	return append([]byte(nil), b...)
}

func cString(mod api.Module, ptr uint32) string {
	if ptr == 0 {
		return ""
	}
	var sb strings.Builder
	for {
		c, ok := memOf(mod).ReadByte(ptr)
		if !ok || c == 0 || sb.Len() > 4096 {
			return sb.String()
		}
		sb.WriteByte(c)
		ptr++
	}
}

// envFunctions implements the "env" imports.
func (rt *Runtime) envFunctions() []hostFn {
	noop := func(context.Context, api.Module, []uint64) {}
	embind := func(name string, params ...api.ValueType) hostFn {
		return hostFn{name, params, nil, noop}
	}
	sys := func(name string, nparams int, errno int32) hostFn {
		p := make([]api.ValueType, nparams)
		for i := range p {
			p[i] = i32
		}
		return hostFn{name, p, []api.ValueType{i32}, func(_ context.Context, _ api.Module, st []uint64) {
			st[0] = api.EncodeI32(-errno)
		}}
	}
	start := time.Now()
	return []hostFn{
		{"__cxa_throw", []api.ValueType{i32, i32, i32}, nil, func(_ context.Context, mod api.Module, st []uint64) {
			// type is a std::type_info*; its second word is the mangled name.
			typ := uint32(st[1])
			name := ""
			if typ != 0 {
				if p, ok := memOf(mod).ReadUint32Le(typ + 4); ok {
					name = cString(mod, p)
				}
			}
			panic(&abortError{"C++ exception thrown (type " + name + ")"})
		}},
		{"abort", nil, nil, func(context.Context, api.Module, []uint64) {
			panic(&abortError{"native code called abort()"})
		}},
		{"strftime", []api.ValueType{i32, i32, i32, i32}, []api.ValueType{i32}, func(_ context.Context, mod api.Module, st []uint64) {
			st[0] = uint64(strftime(mod, uint32(st[0]), uint32(st[1]), uint32(st[2]), uint32(st[3])))
		}},
		{"strftime_l", []api.ValueType{i32, i32, i32, i32, i32}, []api.ValueType{i32}, func(_ context.Context, mod api.Module, st []uint64) {
			st[0] = uint64(strftime(mod, uint32(st[0]), uint32(st[1]), uint32(st[2]), uint32(st[3])))
		}},
		embind("_embind_register_void", i32, i32),
		embind("_embind_register_bool", i32, i32, i32, i32, i32),
		embind("_embind_register_std_string", i32, i32),
		embind("_embind_register_std_wstring", i32, i32, i32),
		embind("_embind_register_emval", i32, i32),
		embind("_embind_register_integer", i32, i32, i32, i32, i32),
		embind("_embind_register_bigint", i32, i32, i32, i64, i64),
		embind("_embind_register_float", i32, i32, i32),
		embind("_embind_register_memory_view", i32, i32, i32),
		// The only files the module opens are the random devices OpenSSL's
		// seed source reads (rand_unix.c: open, fstat, read, and a later
		// fstat to check the device is unchanged). The glue serves them
		// from its in-memory FS (FS.createDevice("/dev", "urandom",
		// randomByte) backed by crypto.getRandomValues); here they are
		// backed by crypto/rand. Without them key generation fails and the
		// first server update crashes on a null key negotiator.
		{"__syscall_openat", []api.ValueType{i32, i32, i32, i32}, []api.ValueType{i32}, func(_ context.Context, mod api.Module, st []uint64) {
			st[0] = api.EncodeI32(rt.openat(mod, cString(mod, uint32(st[1]))))
		}},
		{"__syscall_fstat64", []api.ValueType{i32, i32}, []api.ValueType{i32}, func(_ context.Context, mod api.Module, st []uint64) {
			st[0] = api.EncodeI32(rt.fstat(mod, int32(st[0]), uint32(st[1])))
		}},
		{"__syscall_stat64", []api.ValueType{i32, i32}, []api.ValueType{i32}, func(_ context.Context, mod api.Module, st []uint64) {
			st[0] = api.EncodeI32(rt.statPath(mod, cString(mod, uint32(st[0])), uint32(st[1])))
		}},
		{"__syscall_lstat64", []api.ValueType{i32, i32}, []api.ValueType{i32}, func(_ context.Context, mod api.Module, st []uint64) {
			st[0] = api.EncodeI32(rt.statPath(mod, cString(mod, uint32(st[0])), uint32(st[1])))
		}},
		{"__syscall_newfstatat", []api.ValueType{i32, i32, i32, i32}, []api.ValueType{i32}, func(_ context.Context, mod api.Module, st []uint64) {
			st[0] = api.EncodeI32(rt.statPath(mod, cString(mod, uint32(st[1])), uint32(st[2])))
		}},
		{"__syscall_fcntl64", []api.ValueType{i32, i32, i32}, []api.ValueType{i32}, func(_ context.Context, mod api.Module, st []uint64) {
			if rt.device(mod, int32(st[0])) != nil {
				st[0] = 0 // F_GETFD/F_SETFD and friends: nothing to track
				return
			}
			st[0] = api.EncodeI32(-errnoBADF)
		}},
		sys("__syscall_ioctl", 3, errnoNOTTY),
		sys("__syscall_getdents64", 3, errnoBADF),
		{"emscripten_date_now", nil, []api.ValueType{f64}, func(_ context.Context, _ api.Module, st []uint64) {
			st[0] = api.EncodeF64(float64(time.Now().UnixMicro()) / 1000)
		}},
		{"_emscripten_get_now_is_monotonic", nil, []api.ValueType{i32}, func(_ context.Context, _ api.Module, st []uint64) {
			st[0] = 1
		}},
		{"emscripten_get_now", nil, []api.ValueType{f64}, func(_ context.Context, _ api.Module, st []uint64) {
			st[0] = api.EncodeF64(float64(time.Since(start).Nanoseconds()) / 1e6)
		}},
		{"_localtime_js", []api.ValueType{i64, i32}, nil, func(_ context.Context, mod api.Module, st []uint64) {
			// UTC is the host time zone as far as the module is concerned.
			t := time.Unix(int64(st[0]), 0).UTC()
			p := uint32(st[1])
			for i, v := range []int{t.Second(), t.Minute(), t.Hour(), t.Day(), int(t.Month()) - 1,
				t.Year() - 1900, int(t.Weekday()), t.YearDay() - 1, 0, 0} {
				putU32(mod, p+uint32(4*i), uint32(int32(v)))
			}
		}},
		{"_tzset_js", []api.ValueType{i32, i32, i32}, nil, func(ctx context.Context, mod api.Module, st []uint64) {
			putU32(mod, uint32(st[0]), 0)
			putU32(mod, uint32(st[1]), 0)
			// The glue mallocs the zone names; so do we.
			name := rt.hostMalloc(ctx, mod, []byte("UTC\x00"))
			putU32(mod, uint32(st[2]), name)
			putU32(mod, uint32(st[2])+4, name)
		}},
		{"emscripten_get_heap_max", nil, []api.ValueType{i32}, func(_ context.Context, _ api.Module, st []uint64) {
			st[0] = uint64(rt.info.MemoryMax) * 65536
		}},
		{"emscripten_resize_heap", []api.ValueType{i32}, []api.ValueType{i32}, func(_ context.Context, mod api.Module, st []uint64) {
			// Same policy as the glue's growMemory: grow to at least the
			// requested size (the glue also over-allocates, which is only
			// a performance tweak).
			want := uint64(uint32(st[0]))
			mem := memOf(mod)
			cur := uint64(mem.Size())
			st[0] = 0
			if want <= cur {
				st[0] = 1
				return
			}
			grow := want - cur
			// Over-allocate like emscripten (+20%, capped at 96MiB).
			extra := min(cur/5, 96<<20)
			pages := uint32((grow + extra + 65535) / 65536)
			if _, ok := mem.Grow(pages); ok {
				st[0] = 1
			} else if _, ok = mem.Grow(uint32((grow + 65535) / 65536)); ok {
				st[0] = 1
			}
		}},
	}
}

// wasiFunctions implements the wasi_snapshot_preview1 imports the module
// uses. fd_write collects stdout/stderr lines for the debug logger.
func (rt *Runtime) wasiFunctions() []hostFn {
	return []hostFn{
		{"environ_sizes_get", []api.ValueType{i32, i32}, []api.ValueType{i32}, func(_ context.Context, mod api.Module, st []uint64) {
			putU32(mod, uint32(st[0]), 0)
			putU32(mod, uint32(st[1]), 0)
			st[0] = wasiSuccess
		}},
		{"environ_get", []api.ValueType{i32, i32}, []api.ValueType{i32}, func(_ context.Context, _ api.Module, st []uint64) {
			st[0] = wasiSuccess
		}},
		{"fd_close", []api.ValueType{i32}, []api.ValueType{i32}, func(_ context.Context, mod api.Module, st []uint64) {
			rt.closeFD(mod, int32(st[0]))
			st[0] = wasiSuccess
		}},
		{"fd_sync", []api.ValueType{i32}, []api.ValueType{i32}, func(_ context.Context, _ api.Module, st []uint64) {
			st[0] = wasiSuccess
		}},
		{"fd_read", []api.ValueType{i32, i32, i32, i32}, []api.ValueType{i32}, func(_ context.Context, mod api.Module, st []uint64) {
			fd, iov, n, pnum := int32(st[0]), uint32(st[1]), uint32(st[2]), uint32(st[3])
			putU32(mod, pnum, 0)
			if rt.device(mod, fd) == nil {
				st[0] = errnoBADF
				return
			}
			var total uint32
			for i := uint32(0); i < n; i++ {
				ptr := getU32(mod, iov+8*i)
				l := getU32(mod, iov+8*i+4)
				buf := make([]byte, l)
				if _, err := rand.Read(buf); err != nil {
					panic(&abortError{"crypto/rand: " + err.Error()})
				}
				if !memOf(mod).Write(ptr, buf) {
					panic(&abortError{"fd_read out of bounds"})
				}
				total += l
			}
			putU32(mod, pnum, total)
			st[0] = wasiSuccess
		}},
		{"fd_seek", []api.ValueType{i32, i64, i32, i32}, []api.ValueType{i32}, func(_ context.Context, _ api.Module, st []uint64) {
			st[0] = errnoSPIPE
		}},
		{"fd_write", []api.ValueType{i32, i32, i32, i32}, []api.ValueType{i32}, func(ctx context.Context, mod api.Module, st []uint64) {
			fd, iov, n, pnum := uint32(st[0]), uint32(st[1]), uint32(st[2]), uint32(st[3])
			var total uint32
			var buf []byte
			for i := uint32(0); i < n; i++ {
				ptr := getU32(mod, iov+8*i)
				l := getU32(mod, iov+8*i+4)
				buf = append(buf, readBytes(mod, ptr, l)...)
				total += l
			}
			rt.stdio(ctx, mod, fd, buf)
			putU32(mod, pnum, total)
			st[0] = wasiSuccess
		}},
	}
}

func instantiateHost(ctx context.Context, r wazero.Runtime, name string, fns []hostFn) error {
	b := r.NewHostModuleBuilder(name)
	for _, f := range fns {
		b.NewFunctionBuilder().WithGoModuleFunction(f.fn, f.params, f.results).Export(f.name)
	}
	_, err := b.Instantiate(ctx)
	return err
}

// strftime formats the subset of conversions musl's callers in this module
// can plausibly use (log timestamps). Unknown conversions are copied
// verbatim. Returns the length without the NUL, or 0 if it does not fit.
func strftime(mod api.Module, s, maxsize, format, tmPtr uint32) uint32 {
	f := cString(mod, format)
	tm := make([]int32, 9)
	for i := range tm {
		tm[i] = int32(getU32(mod, tmPtr+uint32(4*i)))
	}
	t := time.Date(int(tm[5])+1900, time.Month(tm[4]+1), int(tm[3]), int(tm[2]), int(tm[1]), int(tm[0]), 0, time.UTC)
	var sb strings.Builder
	for i := 0; i < len(f); i++ {
		if f[i] != '%' || i+1 >= len(f) {
			sb.WriteByte(f[i])
			continue
		}
		i++
		switch f[i] {
		case 'Y':
			fmt.Fprintf(&sb, "%04d", t.Year())
		case 'm':
			fmt.Fprintf(&sb, "%02d", int(t.Month()))
		case 'd':
			fmt.Fprintf(&sb, "%02d", t.Day())
		case 'H':
			fmt.Fprintf(&sb, "%02d", t.Hour())
		case 'M':
			fmt.Fprintf(&sb, "%02d", t.Minute())
		case 'S':
			fmt.Fprintf(&sb, "%02d", t.Second())
		case 'y':
			fmt.Fprintf(&sb, "%02d", t.Year()%100)
		case 'j':
			fmt.Fprintf(&sb, "%03d", t.YearDay())
		case 'F':
			sb.WriteString(t.Format("2006-01-02"))
		case 'T':
			sb.WriteString(t.Format("15:04:05"))
		case 'D':
			sb.WriteString(t.Format("01/02/06"))
		case 'c':
			sb.WriteString(t.Format("Mon Jan  2 15:04:05 2006"))
		case 'a':
			sb.WriteString(t.Format("Mon"))
		case 'b', 'h':
			sb.WriteString(t.Format("Jan"))
		case 'z':
			sb.WriteString("+0000")
		case 'Z':
			sb.WriteString("UTC")
		case 'n':
			sb.WriteByte('\n')
		case 't':
			sb.WriteByte('\t')
		case '%':
			sb.WriteByte('%')
		default:
			sb.WriteByte('%')
			sb.WriteByte(f[i])
		}
	}
	out := sb.String()
	if uint32(len(out))+1 > maxsize {
		return 0
	}
	buf := make([]byte, len(out)+1)
	copy(buf, out)
	if !memOf(mod).Write(s, buf) {
		return 0
	}
	return uint32(len(out))
}

// randomDevice is an open /dev/urandom or /dev/random.
type randomDevice struct {
	path string
	rdev uint32
}

// Device numbers as emscripten's FS gives its /dev/random (1,8) and
// /dev/urandom (1,9) nodes; st_dev/st_ino only need to be stable.
var randomDevices = map[string]uint32{"/dev/random": 1<<8 | 8, "/dev/urandom": 1<<8 | 9}

func (rt *Runtime) openat(mod api.Module, path string) int32 {
	in := rt.instanceOf(mod)
	rdev, ok := randomDevices[path]
	if in == nil || !ok {
		if in != nil {
			in.deferLog("INFO", "framecrypt: module tried to open "+path)
		}
		return -errnoNOENT
	}
	fd := int32(3)
	for in.fds[fd] != nil {
		fd++
	}
	in.fds[fd] = &randomDevice{path: path, rdev: rdev}
	return fd
}

func (rt *Runtime) device(mod api.Module, fd int32) *randomDevice {
	if in := rt.instanceOf(mod); in != nil {
		return in.fds[fd]
	}
	return nil
}

func (rt *Runtime) closeFD(mod api.Module, fd int32) {
	if in := rt.instanceOf(mod); in != nil {
		delete(in.fds, fd)
	}
}

func (rt *Runtime) fstat(mod api.Module, fd int32, buf uint32) int32 {
	d := rt.device(mod, fd)
	if d == nil {
		return -errnoBADF
	}
	writeStat(mod, buf, d.rdev)
	return 0
}

func (rt *Runtime) statPath(mod api.Module, path string, buf uint32) int32 {
	rdev, ok := randomDevices[path]
	if !ok {
		return -errnoNOENT
	}
	writeStat(mod, buf, rdev)
	return 0
}

// writeStat fills a struct stat the way the glue's SYSCALLS.doStat does
// for a character device.
func writeStat(mod api.Module, buf, rdev uint32) {
	zero := make([]byte, 96)
	memOf(mod).Write(buf, zero)
	putU32(mod, buf, 5)                            // st_dev (emscripten's devtmpfs-ish id)
	putU32(mod, buf+4, 0o020000|0o666)             // st_mode: S_IFCHR | rw-rw-rw-
	putU32(mod, buf+8, 1)                          // st_nlink
	putU32(mod, buf+20, rdev)                      // st_rdev
	putU32(mod, buf+32, 4096)                      // st_blksize
	memOf(mod).WriteUint64Le(buf+88, uint64(rdev)) // st_ino
}
