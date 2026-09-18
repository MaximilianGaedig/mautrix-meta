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

package rtcsignal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Type is a Thrift compact protocol wire type.
type Type byte

const (
	TypeStop   Type = 0
	TypeTrue   Type = 1
	TypeFalse  Type = 2
	TypeByte   Type = 3
	TypeI16    Type = 4
	TypeI32    Type = 5
	TypeI64    Type = 6
	TypeDouble Type = 7
	TypeBinary Type = 8
	TypeList   Type = 9
	TypeSet    Type = 10
	TypeMap    Type = 11
	TypeStruct Type = 12
)

const maxDepth = 64

var (
	ErrShortBuffer = errors.New("rtcsignal: unexpected end of thrift data")
	errTooDeep     = errors.New("rtcsignal: thrift nesting too deep")
)

// reader decodes the Thrift compact protocol. The web client (TCompactProtocol
// in the JS bundles) uses stock Apache compact encoding: zigzag varints, field
// id deltas in the high nibble, booleans folded into the field type.
type reader struct {
	b     []byte
	p     int
	depth int
}

func newReader(b []byte) *reader { return &reader{b: b} }

func (r *reader) remaining() int { return len(r.b) - r.p }

func (r *reader) byte() (byte, error) {
	if r.p >= len(r.b) {
		return 0, ErrShortBuffer
	}
	c := r.b[r.p]
	r.p++
	return c, nil
}

func (r *reader) uvarint() (uint64, error) {
	v, n := binary.Uvarint(r.b[r.p:])
	if n <= 0 {
		return 0, ErrShortBuffer
	}
	r.p += n
	return v, nil
}

func (r *reader) i64() (int64, error) {
	v, err := r.uvarint()
	return int64(v>>1) ^ -int64(v&1), err
}

func (r *reader) i32() (int32, error) {
	v, err := r.i64()
	if err == nil && (v > math.MaxInt32 || v < math.MinInt32) {
		err = fmt.Errorf("rtcsignal: i32 out of range: %d", v)
	}
	return int32(v), err
}

func (r *reader) i16() (int16, error) {
	v, err := r.i64()
	if err == nil && (v > math.MaxInt16 || v < math.MinInt16) {
		err = fmt.Errorf("rtcsignal: i16 out of range: %d", v)
	}
	return int16(v), err
}

func (r *reader) binary() ([]byte, error) {
	n, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	if n > uint64(r.remaining()) {
		return nil, ErrShortBuffer
	}
	v := r.b[r.p : r.p+int(n)]
	r.p += int(n)
	return v, nil
}

func (r *reader) string() (string, error) {
	b, err := r.binary()
	return string(b), err
}

// containerBool reads a bool element of a list/set/map, which unlike a bool
// field is written as a full byte (1 = true, 2 = false).
func (r *reader) containerBool() (bool, error) {
	c, err := r.byte()
	return c == byte(TypeTrue), err
}

func (r *reader) listHeader() (Type, int, error) {
	h, err := r.byte()
	if err != nil {
		return 0, 0, err
	}
	n := int(h >> 4)
	if n == 15 {
		v, err := r.uvarint()
		if err != nil {
			return 0, 0, err
		}
		if v > uint64(r.remaining()) {
			return 0, 0, ErrShortBuffer
		}
		n = int(v)
	}
	return Type(h & 0x0f), n, nil
}

func (r *reader) mapHeader() (Type, Type, int, error) {
	v, err := r.uvarint()
	if err != nil {
		return 0, 0, 0, err
	}
	if v == 0 {
		return 0, 0, 0, nil
	}
	if v > uint64(r.remaining()) {
		return 0, 0, 0, ErrShortBuffer
	}
	kv, err := r.byte()
	if err != nil {
		return 0, 0, 0, err
	}
	return Type(kv >> 4), Type(kv & 0x0f), int(v), nil
}

// readStruct iterates over the fields of a struct. The callback must either
// consume the field value or call r.skip(t). Bool fields carry their value in
// the type (TypeTrue/TypeFalse) and have no payload.
func (r *reader) readStruct(fn func(id int16, t Type) error) error {
	r.depth++
	defer func() { r.depth-- }()
	if r.depth > maxDepth {
		return errTooDeep
	}
	var last int16
	for {
		h, err := r.byte()
		if err != nil {
			return err
		}
		if h == 0 {
			return nil
		}
		t := Type(h & 0x0f)
		id := last + int16(h>>4)
		if h>>4 == 0 {
			if id, err = r.i16(); err != nil {
				return err
			}
		}
		last = id
		if err = fn(id, t); err != nil {
			return fmt.Errorf("field %d: %w", id, err)
		}
	}
}

func (r *reader) skip(t Type) error {
	var err error
	switch t {
	case TypeTrue, TypeFalse:
	case TypeByte:
		_, err = r.byte()
	case TypeI16, TypeI32, TypeI64:
		_, err = r.uvarint()
	case TypeDouble:
		if r.remaining() < 8 {
			return ErrShortBuffer
		}
		r.p += 8
	case TypeBinary:
		_, err = r.binary()
	case TypeList, TypeSet:
		var et Type
		var n int
		if et, n, err = r.listHeader(); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err = r.skipElem(et); err != nil {
				return err
			}
		}
	case TypeMap:
		var kt, vt Type
		var n int
		if kt, vt, n, err = r.mapHeader(); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err = r.skipElem(kt); err != nil {
				return err
			}
			if err = r.skipElem(vt); err != nil {
				return err
			}
		}
	case TypeStruct:
		err = r.readStruct(func(_ int16, ft Type) error { return r.skip(ft) })
	default:
		err = fmt.Errorf("rtcsignal: unknown thrift type %d", t)
	}
	return err
}

func (r *reader) skipElem(t Type) error {
	if t == TypeTrue || t == TypeFalse {
		_, err := r.byte()
		return err
	}
	return r.skip(t)
}

// rawStruct consumes a struct and returns its encoded bytes (including the
// stop byte), so unknown message bodies can be kept and re-emitted verbatim.
func (r *reader) rawStruct() ([]byte, error) {
	start := r.p
	if err := r.skip(TypeStruct); err != nil {
		return nil, err
	}
	return r.b[start:r.p], nil
}

func (r *reader) stringList() ([]string, error) {
	_, n, err := r.listHeader()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		s, err := r.string()
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func (r *reader) i32List() ([]int32, error) {
	_, n, err := r.listHeader()
	if err != nil {
		return nil, err
	}
	out := make([]int32, 0, n)
	for i := 0; i < n; i++ {
		v, err := r.i32()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func (r *reader) i16List() ([]int16, error) {
	_, n, err := r.listHeader()
	if err != nil {
		return nil, err
	}
	out := make([]int16, 0, n)
	for i := 0; i < n; i++ {
		v, err := r.i16()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func (r *reader) structList(fn func() error) error {
	_, n, err := r.listHeader()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if err = fn(); err != nil {
			return err
		}
	}
	return nil
}

func (r *reader) stringMap(fn func(key string, vt Type) error) error {
	_, vt, n, err := r.mapHeader()
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		k, err := r.string()
		if err != nil {
			return err
		}
		if err = fn(k, vt); err != nil {
			return err
		}
	}
	return nil
}

// writer encodes the Thrift compact protocol.
type writer struct {
	b    []byte
	last []int16
}

func (w *writer) bytes() []byte { return w.b }

func (w *writer) uvarint(v uint64) { w.b = binary.AppendUvarint(w.b, v) }

func (w *writer) zigzag(v int64) { w.uvarint(uint64((v << 1) ^ (v >> 63))) }

func (w *writer) structBegin() { w.last = append(w.last, 0) }

func (w *writer) structEnd() {
	w.b = append(w.b, 0)
	w.last = w.last[:len(w.last)-1]
}

func (w *writer) fieldHeader(id int16, t Type) {
	top := len(w.last) - 1
	delta := int(id) - int(w.last[top])
	if delta > 0 && delta <= 15 {
		w.b = append(w.b, byte(delta<<4)|byte(t))
	} else {
		w.b = append(w.b, byte(t))
		w.zigzag(int64(id))
	}
	w.last[top] = id
}

func (w *writer) binaryValue(v []byte) {
	w.uvarint(uint64(len(v)))
	w.b = append(w.b, v...)
}

func (w *writer) fieldBinary(id int16, v []byte) {
	w.fieldHeader(id, TypeBinary)
	w.binaryValue(v)
}

func (w *writer) fieldString(id int16, v string) { w.fieldBinary(id, []byte(v)) }

// optString writes a string field only when it is non-empty.
func (w *writer) optString(id int16, v string) {
	if v != "" {
		w.fieldString(id, v)
	}
}

func (w *writer) fieldI16(id int16, v int16) {
	w.fieldHeader(id, TypeI16)
	w.zigzag(int64(v))
}

func (w *writer) fieldI32(id int16, v int32) {
	w.fieldHeader(id, TypeI32)
	w.zigzag(int64(v))
}

func (w *writer) fieldI64(id int16, v int64) {
	w.fieldHeader(id, TypeI64)
	w.zigzag(v)
}

func (w *writer) fieldBool(id int16, v bool) {
	if v {
		w.fieldHeader(id, TypeTrue)
	} else {
		w.fieldHeader(id, TypeFalse)
	}
}

func (w *writer) listHeader(et Type, n int) {
	if n < 15 {
		w.b = append(w.b, byte(n<<4)|byte(et))
	} else {
		w.b = append(w.b, 0xf0|byte(et))
		w.uvarint(uint64(n))
	}
}

func (w *writer) mapHeader(kt, vt Type, n int) {
	w.uvarint(uint64(n))
	if n > 0 {
		w.b = append(w.b, byte(kt)<<4|byte(vt))
	}
}

func (w *writer) fieldStringList(id int16, t Type, v []string) {
	w.fieldHeader(id, t)
	w.listHeader(TypeBinary, len(v))
	for _, s := range v {
		w.binaryValue([]byte(s))
	}
}

func (w *writer) fieldI32List(id int16, t Type, v []int32) {
	w.fieldHeader(id, t)
	w.listHeader(TypeI32, len(v))
	for _, x := range v {
		w.zigzag(int64(x))
	}
}

func (w *writer) fieldI16List(id int16, v []int16) {
	w.fieldHeader(id, TypeList)
	w.listHeader(TypeI16, len(v))
	for _, x := range v {
		w.zigzag(int64(x))
	}
}

// fieldStruct writes a nested struct field whose contents are produced by fn.
func (w *writer) fieldStruct(id int16, fn func()) {
	w.fieldHeader(id, TypeStruct)
	w.structBegin()
	fn()
	w.structEnd()
}

// fieldRawStruct writes a nested struct field from its already-encoded bytes
// (which must include the trailing stop byte).
func (w *writer) fieldRawStruct(id int16, raw []byte) {
	w.fieldHeader(id, TypeStruct)
	w.b = append(w.b, raw...)
}
