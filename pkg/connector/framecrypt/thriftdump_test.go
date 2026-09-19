package framecrypt

import (
	"fmt"
	"strings"
)

// dumpCompact renders a Thrift compact struct as a field tree for
// debugging. Binary values are shown by length only (they are keys).
func dumpCompact(b []byte) string {
	d := &cdump{b: b}
	var sb strings.Builder
	func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(&sb, " <error at %d: %v>", d.p, r)
			}
		}()
		d.structv(&sb, 0)
	}()
	return sb.String()
}

type cdump struct {
	b []byte
	p int
}

func (d *cdump) byte1() byte { c := d.b[d.p]; d.p++; return c }
func (d *cdump) varint() uint64 {
	var v uint64
	for s := 0; ; s += 7 {
		c := d.byte1()
		v |= uint64(c&0x7f) << s
		if c < 0x80 {
			return v
		}
	}
}
func (d *cdump) zz() int64 { v := d.varint(); return int64(v>>1) ^ -int64(v&1) }

func (d *cdump) structv(sb *strings.Builder, ind int) {
	sb.WriteString("{\n")
	last := int16(0)
	for {
		h := d.byte1()
		if h == 0 {
			break
		}
		t := h & 0xf
		var id int16
		if h>>4 != 0 {
			id = last + int16(h>>4)
		} else {
			id = int16(d.zz())
		}
		last = id
		fmt.Fprintf(sb, "%s%d: ", strings.Repeat("  ", ind+1), id)
		d.value(sb, t, ind+1)
		sb.WriteString("\n")
	}
	sb.WriteString(strings.Repeat("  ", ind) + "}")
}

func (d *cdump) value(sb *strings.Builder, t byte, ind int) {
	switch t {
	case 1:
		sb.WriteString("true")
	case 2:
		sb.WriteString("false")
	case 3:
		fmt.Fprintf(sb, "byte %d", int8(d.byte1()))
	case 4, 5, 6:
		fmt.Fprintf(sb, "i%d %d", map[byte]int{4: 16, 5: 32, 6: 64}[t], d.zz())
	case 7:
		d.p += 8
		sb.WriteString("double")
	case 8:
		n := int(d.varint())
		s := d.b[d.p : d.p+n]
		d.p += n
		printable := n > 0 && n < 40
		for _, c := range s {
			if c < 0x20 || c > 0x7e {
				printable = false
			}
		}
		if printable {
			fmt.Fprintf(sb, "str(%d) %q", n, redact(string(s)))
		} else {
			fmt.Fprintf(sb, "bin(%d)", n)
		}
	case 9, 10:
		h := d.byte1()
		n, et := int(h>>4), h&0xf
		if n == 15 {
			n = int(d.varint())
		}
		fmt.Fprintf(sb, "list<%d>[%d] ", et, n)
		for i := 0; i < n; i++ {
			if et == 1 || et == 2 {
				fmt.Fprintf(sb, "%v ", d.byte1() == 1)
				continue
			}
			d.value(sb, et, ind)
			sb.WriteString(" ")
		}
	case 11:
		n := int(d.varint())
		if n == 0 {
			sb.WriteString("map{}")
			return
		}
		kv := d.byte1()
		fmt.Fprintf(sb, "map<%d,%d>[%d] ", kv>>4, kv&0xf, n)
		for i := 0; i < n; i++ {
			d.value(sb, kv>>4, ind)
			sb.WriteString(" => ")
			d.value(sb, kv&0xf, ind)
			sb.WriteString("; ")
		}
	case 12:
		d.structv(sb, ind)
	default:
		panic(fmt.Sprintf("type %d", t))
	}
}

// redact hides digits of user ids and the cname part of "<uid>:<cname>".
func redact(s string) string {
	uid, cname, ok := strings.Cut(s, ":")
	if ok && len(uid) > 6 {
		return fmt.Sprintf("<uid%d>:<cname%d>", len(uid), len(cname))
	}
	if len(s) > 8 && strings.Trim(s, "0123456789") == "" {
		return fmt.Sprintf("<num%d>", len(s))
	}
	return s
}
