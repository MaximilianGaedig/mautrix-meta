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
	"bytes"
	"errors"
	"testing"
)

func TestCompactPrimitives(t *testing.T) {
	w := &writer{}
	w.structBegin()
	w.fieldI32(1, -1)
	w.fieldI64(2, 1<<40)
	w.fieldBool(3, true)
	w.fieldBool(4, false)
	w.fieldString(40, "long delta") // delta > 15 forces the long field header
	w.fieldI16(41, -300)
	many := make([]string, 20) // > 14 elements forces the long list header
	for i := range many {
		many[i] = string(rune('a' + i))
	}
	w.fieldStringList(42, TypeList, many)
	w.fieldStruct(43, func() { w.fieldString(1, "nested") })
	w.structEnd()

	got := map[int16]any{}
	r := newReader(w.bytes())
	err := r.readStruct(func(id int16, typ Type) (err error) {
		switch id {
		case 1:
			got[id], err = r.i32()
		case 2:
			got[id], err = r.i64()
		case 3, 4:
			got[id] = typ == TypeTrue
		case 40:
			got[id], err = r.string()
		case 41:
			got[id], err = r.i16()
		case 42:
			got[id], err = r.stringList()
		case 43:
			err = r.readStruct(func(id int16, typ Type) (err error) {
				got[100+id], err = r.string()
				return
			})
		default:
			t.Errorf("unexpected field %d", id)
			err = r.skip(typ)
		}
		return
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.remaining() != 0 {
		t.Fatalf("%d bytes left", r.remaining())
	}
	if got[1] != int32(-1) || got[2] != int64(1<<40) || got[3] != true || got[4] != false ||
		got[40] != "long delta" || got[41] != int16(-300) || got[101] != "nested" {
		t.Fatalf("bad values: %v", got)
	}
	if l := got[42].([]string); len(l) != 20 || l[19] != "t" {
		t.Fatalf("bad list: %v", l)
	}
}

// Known compact encodings, checked by hand against the Apache spec.
func TestCompactWireFormat(t *testing.T) {
	w := &writer{}
	w.structBegin()
	w.fieldI32(1, 15)                          // 0x15 zigzag(15)=30=0x1e
	w.fieldString(3, "ab")                     // delta 2, binary: 0x28 0x02 'a' 'b'
	w.fieldBool(4, true)                       // delta 1, true: 0x11
	w.fieldI64(20, -1)                         // delta 16 -> long form: 0x06, zigzag i16 20=40=0x28, zigzag(-1)=1
	w.fieldI32List(21, TypeSet, []int32{1, 2}) // 0x1a, header 0x25, 2, 4
	w.structEnd()
	want := []byte{0x15, 0x1e, 0x28, 0x02, 'a', 'b', 0x11, 0x06, 0x28, 0x01, 0x1a, 0x25, 0x02, 0x04, 0x00}
	if !bytes.Equal(w.bytes(), want) {
		t.Fatalf("got % x\nwant % x", w.bytes(), want)
	}
}

func TestCompactSkipAndBoolInMap(t *testing.T) {
	// map<string,bool>{"x": true} as the web client writes JoinRequest.mediaStatus,
	// followed by a double, a nested list of structs and a map<i32,struct>.
	w := &writer{}
	w.structBegin()
	w.fieldHeader(4, TypeMap)
	w.mapHeader(TypeBinary, TypeTrue, 1)
	w.binaryValue([]byte("x"))
	w.b = append(w.b, byte(TypeTrue))
	w.fieldHeader(5, TypeDouble)
	w.b = append(w.b, 0, 0, 0, 0, 0, 0, 0xf0, 0x3f)
	w.fieldHeader(6, TypeList)
	w.listHeader(TypeStruct, 2)
	for i := 0; i < 2; i++ {
		w.structBegin()
		w.fieldBool(1, i == 0)
		w.structEnd()
	}
	w.fieldHeader(7, TypeMap)
	w.mapHeader(TypeI32, TypeStruct, 1)
	w.zigzag(3)
	w.structBegin()
	w.structEnd()
	w.fieldString(8, "end")
	w.structEnd()

	r := newReader(w.bytes())
	var end string
	err := r.readStruct(func(id int16, typ Type) (err error) {
		if id == 8 {
			end, err = r.string()
			return
		}
		return r.skip(typ)
	})
	if err != nil || end != "end" || r.remaining() != 0 {
		t.Fatalf("skip failed: err=%v end=%q left=%d", err, end, r.remaining())
	}
}

func TestCompactTruncated(t *testing.T) {
	data, err := EncodePayload(fakeJoin([]byte{0}))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(data); i++ {
		if _, err := DecodePayload(data[:i], false); err == nil {
			t.Fatalf("prefix of %d/%d bytes decoded without error", i, len(data))
		}
	}
	if _, err := DecodePayload(append(bytes.Clone(data), 0), false); err == nil {
		t.Fatal("trailing byte accepted")
	}
}

func TestCompactDepthLimit(t *testing.T) {
	var b []byte
	for i := 0; i < maxDepth+2; i++ {
		b = append(b, 0x1c) // field 1, struct
	}
	r := newReader(b)
	if err := r.skip(TypeStruct); !errors.Is(err, errTooDeep) {
		t.Fatalf("expected depth error, got %v", err)
	}
}
