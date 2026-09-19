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
	"bytes"
	"errors"
	"fmt"
)

// Binary section ids (WebAssembly core spec, 5.5.2).
const (
	secCustom   = 0
	secType     = 1
	secImport   = 2
	secFunction = 3
	secTable    = 4
	secMemory   = 5
	secGlobal   = 6
	secExport   = 7
	secStart    = 8
	secElement  = 9
	secCode     = 10
	secData     = 11
	secDataCnt  = 12
)

// sectionOrder is the position of each non-custom section in a valid module.
var sectionOrder = map[byte]int{
	secType: 1, secImport: 2, secFunction: 3, secTable: 4, secMemory: 5,
	secGlobal: 6, secExport: 7, secStart: 8, secElement: 9, secDataCnt: 10,
	secCode: 11, secData: 12,
}

var wasmHeader = []byte{0x00, 'a', 's', 'm', 0x01, 0x00, 0x00, 0x00}

type wasmSection struct {
	id   byte
	body []byte
}

type limits struct {
	min    uint32
	max    uint32
	hasMax bool
}

// RewriteInfo describes what rewriteModule changed.
type RewriteInfo struct {
	// Memory is the limits of the memory import that became a defined memory.
	MemoryMin, MemoryMax uint32
	// TableBase is the original minimum size of the function table, i.e.
	// the first index of the slots added for host callbacks.
	TableBase uint32
	// TableSize is the new minimum table size.
	TableSize uint32
}

// rewriteModule makes the emscripten build self-contained enough for a
// non-JS host, changing nothing but two declarations:
//
//   - The "env"."memory" import (the web client passes a shared
//     WebAssembly.Memory, see WebAssemblyMemorySingleton) becomes a memory
//     defined by the module with the same limits. Memory imports don't take
//     part in the function index space, so no code changes.
//   - The module's own function table (exported as
//     __indirect_function_table) grows by extraSlots. The JS glue's
//     addFunction appends JS callbacks to that table at run time
//     (getEmptyTableSlot → wasmTable.grow(1)); wazero cannot insert host
//     functions into a table from Go, so the slots exist from the start and
//     a separate helper module fills them with host functions through an
//     element segment (see buildShim).
//
// Adding imported functions to the module itself would shift every function
// index and require re-encoding all code, which is why the callbacks live
// in a helper module instead.
func rewriteModule(in []byte, extraSlots uint32) ([]byte, *RewriteInfo, error) {
	secs, err := splitSections(in)
	if err != nil {
		return nil, nil, err
	}
	info := &RewriteInfo{}
	var memLimits *limits
	tableDone := false
	for i := range secs {
		switch secs[i].id {
		case secImport:
			var body []byte
			body, memLimits, err = dropMemoryImport(secs[i].body)
			if err != nil {
				return nil, nil, fmt.Errorf("import section: %w", err)
			}
			secs[i].body = body
		case secMemory:
			return nil, nil, errors.New("module already defines a memory")
		case secTable:
			body, base, size, err := growTable(secs[i].body, extraSlots)
			if err != nil {
				return nil, nil, fmt.Errorf("table section: %w", err)
			}
			secs[i].body = body
			info.TableBase, info.TableSize = base, size
			tableDone = true
		}
	}
	if memLimits == nil {
		return nil, nil, errors.New(`no "env"."memory" import`)
	}
	if !tableDone {
		return nil, nil, errors.New("module defines no function table")
	}
	info.MemoryMin, info.MemoryMax = memLimits.min, memLimits.max

	memBody := appendU32(nil, 1)
	memBody = appendLimits(memBody, *memLimits)
	memSec := wasmSection{id: secMemory, body: memBody}
	out := make([]wasmSection, 0, len(secs)+1)
	inserted := false
	for _, s := range secs {
		if !inserted && s.id != secCustom && sectionOrder[s.id] > sectionOrder[secMemory] {
			out = append(out, memSec)
			inserted = true
		}
		out = append(out, s)
	}
	if !inserted {
		out = append(out, memSec)
	}
	return joinSections(out), info, nil
}

func splitSections(in []byte) ([]wasmSection, error) {
	if !bytes.HasPrefix(in, wasmHeader) {
		return nil, errors.New("not a wasm v1 binary")
	}
	r := &wreader{b: in, p: len(wasmHeader)}
	var secs []wasmSection
	for r.p < len(r.b) {
		id := r.byte1()
		n := r.u32()
		body := r.take(int(n))
		if r.err != nil {
			return nil, fmt.Errorf("section %d: %w", id, r.err)
		}
		secs = append(secs, wasmSection{id: id, body: body})
	}
	return secs, nil
}

func joinSections(secs []wasmSection) []byte {
	out := append([]byte{}, wasmHeader...)
	for _, s := range secs {
		out = append(out, s.id)
		out = appendU32(out, uint32(len(s.body)))
		out = append(out, s.body...)
	}
	return out
}

// dropMemoryImport removes the env.memory import and returns its limits.
// Every other import is copied byte for byte.
func dropMemoryImport(body []byte) ([]byte, *limits, error) {
	r := &wreader{b: body}
	n := r.u32()
	var out []byte
	var mem *limits
	kept := uint32(0)
	for i := uint32(0); i < n && r.err == nil; i++ {
		start := r.p
		mod := r.name()
		field := r.name()
		kind := r.byte1()
		switch kind {
		case 0x00: // func
			r.u32()
		case 0x01: // table
			r.byte1()
			r.limits()
		case 0x02: // memory
			l := r.limits()
			if mod == "env" && field == "memory" {
				mem = &l
				continue
			}
		case 0x03: // global
			r.byte1()
			r.byte1()
		default:
			return nil, nil, fmt.Errorf("unknown import kind %#x", kind)
		}
		out = append(out, r.b[start:r.p]...)
		kept++
	}
	if r.err != nil {
		return nil, nil, r.err
	}
	if r.p != len(body) {
		return nil, nil, errors.New("trailing bytes")
	}
	return append(appendU32(nil, kept), out...), mem, nil
}

// growTable raises the minimum (and a lower maximum) of table 0.
func growTable(body []byte, extra uint32) ([]byte, uint32, uint32, error) {
	r := &wreader{b: body}
	n := r.u32()
	if n < 1 {
		return nil, 0, 0, errors.New("empty table section")
	}
	refType := r.byte1()
	if refType != 0x70 {
		return nil, 0, 0, fmt.Errorf("table 0 is not funcref (%#x)", refType)
	}
	l := r.limits()
	if r.err != nil {
		return nil, 0, 0, r.err
	}
	base := l.min
	l.min += extra
	if l.hasMax && l.max < l.min {
		l.max = l.min
	}
	out := appendU32(nil, n)
	out = append(out, refType)
	out = appendLimits(out, l)
	out = append(out, r.b[r.p:]...)
	return out, base, l.min, nil
}

type wreader struct {
	b   []byte
	p   int
	err error
}

func (r *wreader) fail(err error) {
	if r.err == nil {
		r.err = err
	}
}

func (r *wreader) byte1() byte {
	if r.err != nil || r.p >= len(r.b) {
		r.fail(errors.New("unexpected end"))
		return 0
	}
	c := r.b[r.p]
	r.p++
	return c
}

func (r *wreader) u32() uint32 {
	var v uint32
	for shift := 0; shift < 35; shift += 7 {
		c := r.byte1()
		v |= uint32(c&0x7f) << shift
		if c < 0x80 {
			return v
		}
	}
	r.fail(errors.New("bad leb128"))
	return 0
}

func (r *wreader) take(n int) []byte {
	if r.err != nil || n < 0 || r.p+n > len(r.b) {
		r.fail(errors.New("unexpected end"))
		return nil
	}
	s := r.b[r.p : r.p+n]
	r.p += n
	return s
}

func (r *wreader) name() string { return string(r.take(int(r.u32()))) }

func (r *wreader) limits() limits {
	flag := r.byte1()
	l := limits{min: r.u32()}
	switch flag {
	case 0x00:
	case 0x01:
		l.max, l.hasMax = r.u32(), true
	default:
		r.fail(fmt.Errorf("unsupported limits flag %#x", flag))
	}
	return l
}

func appendU32(b []byte, v uint32) []byte {
	for {
		c := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			b = append(b, c|0x80)
		} else {
			return append(b, c)
		}
	}
}

func appendLimits(b []byte, l limits) []byte {
	if l.hasMax {
		b = append(b, 0x01)
		b = appendU32(b, l.min)
		return appendU32(b, l.max)
	}
	b = append(b, 0x00)
	return appendU32(b, l.min)
}

func appendName(b []byte, s string) []byte {
	b = appendU32(b, uint32(len(s)))
	return append(b, s...)
}

// Wasm value type bytes.
const (
	valI32 = 0x7f
	valI64 = 0x7e
	valF32 = 0x7d
	valF64 = 0x7c
)

// callbackSig is an emscripten addFunction signature ("viii" = void
// return, three i32 params). Only i32 is used by this module's callbacks.
type callbackSig string

func (s callbackSig) params() int { return len(s) - 1 }
func (s callbackSig) hasResult() bool {
	return s[0] == 'i'
}

// buildShim encodes the helper module that places host callbacks into the
// main module's table: it imports len(sigs) host functions from hostModule
// (named "s0", "s1", ...), imports the table exported by mainModule, and
// has one active element segment writing the functions to consecutive
// indices starting at base.
func buildShim(hostModule, mainModule string, sigs []callbackSig, tableMin, base uint32) []byte {
	typeIdx := map[callbackSig]uint32{}
	var types []callbackSig
	for _, s := range sigs {
		if _, ok := typeIdx[s]; !ok {
			typeIdx[s] = uint32(len(types))
			types = append(types, s)
		}
	}
	var secs []wasmSection

	tb := appendU32(nil, uint32(len(types)))
	for _, s := range types {
		tb = append(tb, 0x60)
		tb = appendU32(tb, uint32(s.params()))
		for i := 0; i < s.params(); i++ {
			tb = append(tb, valI32)
		}
		if s.hasResult() {
			tb = append(tb, 1, valI32)
		} else {
			tb = append(tb, 0)
		}
	}
	secs = append(secs, wasmSection{secType, tb})

	ib := appendU32(nil, uint32(len(sigs)+1))
	for i, s := range sigs {
		ib = appendName(ib, hostModule)
		ib = appendName(ib, fmt.Sprintf("s%d", i))
		ib = append(ib, 0x00)
		ib = appendU32(ib, typeIdx[s])
	}
	ib = appendName(ib, mainModule)
	ib = appendName(ib, "__indirect_function_table")
	ib = append(ib, 0x01, 0x70)
	ib = appendLimits(ib, limits{min: tableMin})
	secs = append(secs, wasmSection{secImport, ib})

	eb := appendU32(nil, 1)
	eb = append(eb, 0x00)           // active, table 0, funcidx vector
	eb = append(eb, 0x41)           // i32.const
	eb = appendS32(eb, int32(base)) // offset
	eb = append(eb, 0x0b)           // end
	eb = appendU32(eb, uint32(len(sigs)))
	for i := range sigs {
		eb = appendU32(eb, uint32(i))
	}
	secs = append(secs, wasmSection{secElement, eb})
	return joinSections(secs)
}

func appendS32(b []byte, v int32) []byte {
	for {
		c := byte(v & 0x7f)
		v >>= 7
		if (v == 0 && c&0x40 == 0) || (v == -1 && c&0x40 != 0) {
			return append(b, c)
		}
		b = append(b, c|0x80)
	}
}
