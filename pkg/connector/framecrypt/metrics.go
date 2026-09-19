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
	"errors"
)

// errBadThrift is returned for malformed Thrift compact data.
var errBadThrift = errors.New("framecrypt: malformed thrift compact data")

// groupMetricInts extracts the integer fields of GroupE2eeMetrics
// (E2eeMetrics field 2) from a serialized E2eeMetrics. Field ids are those
// of E2eeMetricsSerializers in the web client.
func groupMetricInts(data []byte) (map[int16]int64, error) {
	r := &tcReader{b: data}
	out := map[int16]int64{}
	err := r.structFields(func(id int16, t byte) error {
		if id != 2 || t != 12 {
			return r.skip(t)
		}
		return r.structFields(func(id int16, t byte) error {
			switch t {
			case 3, 4, 5, 6:
				v, err := r.int(t)
				out[id] = v
				return err
			}
			return r.skip(t)
		})
	})
	return out, err
}

// tcReader is a minimal Thrift compact-protocol reader.
type tcReader struct {
	b []byte
	p int
}

func (r *tcReader) byte1() (byte, error) {
	if r.p >= len(r.b) {
		return 0, errBadThrift
	}
	c := r.b[r.p]
	r.p++
	return c, nil
}

func (r *tcReader) varint() (uint64, error) {
	var v uint64
	for s := 0; s < 64; s += 7 {
		c, err := r.byte1()
		if err != nil {
			return 0, err
		}
		v |= uint64(c&0x7f) << s
		if c < 0x80 {
			return v, nil
		}
	}
	return 0, errBadThrift
}

func (r *tcReader) zigzag() (int64, error) {
	v, err := r.varint()
	return int64(v>>1) ^ -int64(v&1), err
}

func (r *tcReader) int(t byte) (int64, error) {
	if t == 3 {
		c, err := r.byte1()
		return int64(int8(c)), err
	}
	return r.zigzag()
}

func (r *tcReader) structFields(f func(id int16, t byte) error) error {
	var last int16
	for {
		h, err := r.byte1()
		if err != nil {
			return err
		}
		if h == 0 {
			return nil
		}
		t := h & 0x0f
		id := last + int16(h>>4)
		if h>>4 == 0 {
			v, err := r.zigzag()
			if err != nil {
				return err
			}
			id = int16(v)
		}
		last = id
		if t == 1 || t == 2 {
			continue // bool value is in the type nibble
		}
		if err = f(id, t); err != nil {
			return err
		}
	}
}

func (r *tcReader) skip(t byte) error {
	switch t {
	case 1, 2:
		return nil
	case 3:
		_, err := r.byte1()
		return err
	case 4, 5, 6:
		_, err := r.varint()
		return err
	case 7:
		if r.p+8 > len(r.b) {
			return errBadThrift
		}
		r.p += 8
		return nil
	case 8:
		n, err := r.varint()
		if err != nil || uint64(len(r.b)-r.p) < n {
			return errBadThrift
		}
		r.p += int(n)
		return nil
	case 9, 10:
		h, err := r.byte1()
		if err != nil {
			return err
		}
		n, et := uint64(h>>4), h&0x0f
		if n == 15 {
			if n, err = r.varint(); err != nil {
				return err
			}
		}
		for i := uint64(0); i < n; i++ {
			if et == 1 || et == 2 {
				if _, err = r.byte1(); err != nil {
					return err
				}
				continue
			}
			if err = r.skip(et); err != nil {
				return err
			}
		}
		return nil
	case 11:
		n, err := r.varint()
		if err != nil || n == 0 {
			return err
		}
		kv, err := r.byte1()
		if err != nil {
			return err
		}
		for i := uint64(0); i < n; i++ {
			if err = r.skip(kv >> 4); err != nil {
				return err
			}
			if err = r.skip(kv & 0x0f); err != nil {
				return err
			}
		}
		return nil
	case 12:
		return r.structFields(func(_ int16, t byte) error { return r.skip(t) })
	}
	return errBadThrift
}
