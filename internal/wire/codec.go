// Package wire is the binary encoding shared by the engine, relays and the
// TypeScript client (ts/transport/src/codec.ts mirrors it byte for byte).
//
// Values are positional and typed by the unit's column descriptor, never
// self-describing:
//
//	row      = null bitmap (ceil(n/8) bytes, bit i set = column i NULL) value*
//	int      = zigzag varint
//	float    = 8 bytes little-endian IEEE 754
//	bool     = 1 byte
//	uuid     = 16 bytes
//	ts       = zigzag varint microseconds since 1970 (+/-infinity: max/min int64)
//	array    = varint length + JSON text (elements typed by the element kind)
//	other    = varint length + Postgres text form (numeric, date, json, text...)
//	pk       = value* of the primary key columns, no bitmap (never NULL)
//
// Inputs are Postgres text output (pgoutput and text-format queries, with
// TimeZone=UTC and DateStyle=ISO).
package wire

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/nightridefm/localsync/internal/catalog"
)

// Column is what the encoder needs to know about one projected column.
type Column struct {
	Name string
	Kind catalog.Kind
	Elem catalog.Kind
}

func AppendUvarint(dst []byte, v uint64) []byte { return binary.AppendUvarint(dst, v) }

func appendZigzag(dst []byte, v int64) []byte {
	return binary.AppendUvarint(dst, uint64(v<<1)^uint64(v>>63))
}

// AppendValue encodes one non-NULL value given as Postgres text.
func AppendValue(dst []byte, c Column, v []byte) ([]byte, error) {
	switch c.Kind {
	case catalog.Int:
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return dst, err
		}
		return appendZigzag(dst, n), nil
	case catalog.Float:
		f, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return dst, err
		}
		return binary.LittleEndian.AppendUint64(dst, math.Float64bits(f)), nil
	case catalog.Bool:
		if len(v) == 1 && v[0] == 't' {
			return append(dst, 1), nil
		}
		return append(dst, 0), nil
	case catalog.UUID:
		return appendUUID(dst, v)
	case catalog.Timestamp:
		us, err := ParseTimestamp(v)
		if err != nil {
			return dst, err
		}
		return appendZigzag(dst, us), nil
	case catalog.Array:
		j, ok := ArrayJSON(string(v), c.Elem)
		if !ok {
			j = string(v)
		}
		dst = binary.AppendUvarint(dst, uint64(len(j)))
		return append(dst, j...), nil
	}
	dst = binary.AppendUvarint(dst, uint64(len(v)))
	return append(dst, v...), nil
}

func appendUUID(dst []byte, v []byte) ([]byte, error) {
	var out [16]byte
	n := 0
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '-' {
			continue
		}
		h, ok := hexVal(c)
		if !ok || n >= 32 {
			return dst, errors.New("bad uuid")
		}
		if n%2 == 0 {
			out[n/2] = h << 4
		} else {
			out[n/2] |= h
		}
		n++
	}
	if n != 32 {
		return dst, errors.New("bad uuid")
	}
	return append(dst, out[:]...), nil
}

func hexVal(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// ParseTimestamp reads 'YYYY-MM-DD HH:MM:SS[.ffffff][+HH[:MM[:SS]]]' (an
// offset-less value is UTC) into microseconds since the epoch.
func ParseTimestamp(v []byte) (int64, error) {
	s := string(v)
	switch s {
	case "infinity":
		return math.MaxInt64, nil
	case "-infinity":
		return math.MinInt64, nil
	}
	if len(s) < 19 || s[4] != '-' || s[7] != '-' || (s[10] != ' ' && s[10] != 'T') || s[13] != ':' || s[16] != ':' {
		return 0, errors.New("bad timestamp")
	}
	num := func(a, b int) int {
		n := 0
		for i := a; i < b; i++ {
			n = n*10 + int(s[i]-'0')
		}
		return n
	}
	t := time.Date(num(0, 4), time.Month(num(5, 7)), num(8, 10), num(11, 13), num(14, 16), num(17, 19), 0, time.UTC)
	us := t.Unix() * 1_000_000
	i := 19
	if i < len(s) && s[i] == '.' {
		j := i + 1
		frac := 0
		digits := 0
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			if digits < 6 {
				frac = frac*10 + int(s[j]-'0')
				digits++
			}
			j++
		}
		for ; digits < 6; digits++ {
			frac *= 10
		}
		us += int64(frac)
		i = j
	}
	if i < len(s) {
		sign := int64(1)
		switch s[i] {
		case '+':
		case '-':
			sign = -1
		case 'Z':
			return us, nil
		default:
			return 0, errors.New("bad timestamp offset")
		}
		parts := strings.Split(s[i+1:], ":")
		secs := int64(0)
		mult := []int64{3600, 60, 1}
		for k, p := range parts {
			if k > 2 {
				break
			}
			n, err := strconv.Atoi(p)
			if err != nil {
				return 0, err
			}
			secs += int64(n) * mult[k]
		}
		us -= sign * secs * 1_000_000
	}
	return us, nil
}

// ArrayJSON converts a 1-D Postgres array literal to JSON. Elements of
// numeric kinds become numbers, bools true/false, everything else strings.
func ArrayJSON(s string, elem catalog.Kind) (string, bool) {
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return "", false
	}
	var b strings.Builder
	b.WriteByte('[')
	i, end := 1, len(s)-1
	first := true
	for i < end {
		if !first {
			b.WriteByte(',')
		}
		first = false
		if s[i] == '"' {
			var v strings.Builder
			i++
			for i < end && s[i] != '"' {
				if s[i] == '\\' {
					i++
				}
				v.WriteByte(s[i])
				i++
			}
			i++
			writeElem(&b, v.String(), true, elem)
		} else {
			j := i
			for j < end && s[j] != ',' {
				j++
			}
			raw := s[i:j]
			if strings.HasPrefix(raw, "{") {
				return "", false
			}
			writeElem(&b, raw, false, elem)
			i = j
		}
		if i < end {
			if s[i] != ',' {
				return "", false
			}
			i++
		}
	}
	b.WriteByte(']')
	return b.String(), true
}

func writeElem(b *strings.Builder, v string, quoted bool, elem catalog.Kind) {
	if !quoted && v == "NULL" {
		b.WriteString("null")
		return
	}
	switch elem {
	case catalog.Int, catalog.Float, catalog.Numeric:
		// NaN / Infinity elements of float arrays are not JSON numbers.
		if isJSONNumber(v) {
			b.WriteString(v)
			return
		}
	case catalog.Bool:
		if v == "t" {
			b.WriteString("true")
			return
		}
		if v == "f" {
			b.WriteString("false")
			return
		}
	}
	writeJSONString(b, v)
}

// isJSONNumber: -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?
func isJSONNumber(s string) bool {
	i := 0
	if i < len(s) && s[i] == '-' {
		i++
	}
	digits := func() int {
		n := 0
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
			n++
		}
		return n
	}
	switch {
	case i < len(s) && s[i] == '0':
		i++
	case digits() == 0:
		return false
	}
	if i < len(s) && s[i] == '.' {
		i++
		if digits() == 0 {
			return false
		}
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		if digits() == 0 {
			return false
		}
	}
	return i == len(s)
}

// writeJSONString quotes s as a JSON string. Invalid UTF-8 becomes U+FFFD
// (strconv.Quote would emit Go escapes such as \xc5, which JSON rejects).
func writeJSONString(b *strings.Builder, s string) {
	const hex = "0123456789abcdef"
	b.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '"' || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r < 0x20:
			b.WriteString(`\u00`)
			b.WriteByte(hex[r>>4])
			b.WriteByte(hex[r&0xf])
		default:
			// Ranging over a string yields U+FFFD for invalid bytes.
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
}

// Projection encodes rows of one table for one unit's column list.
type Projection struct {
	Cols []Column
	// Index of each projected column in the table row, and of the PK
	// columns within the projection.
	Src []int
	PK  []int
}

// AppendRow encodes row (in table column order) for this projection.
func (p *Projection) AppendRow(dst []byte, row [][]byte) ([]byte, error) {
	start := len(dst)
	nb := (len(p.Cols) + 7) / 8
	for range nb {
		dst = append(dst, 0)
	}
	var err error
	for i, src := range p.Src {
		v := row[src]
		if v == nil {
			dst[start+i/8] |= 1 << (i % 8)
			continue
		}
		if dst, err = AppendValue(dst, p.Cols[i], v); err != nil {
			return dst, err
		}
	}
	return dst, nil
}

// AppendPK encodes the primary key of row (table column order).
func (p *Projection) AppendPK(dst []byte, row [][]byte) ([]byte, error) {
	var err error
	for _, i := range p.PK {
		if dst, err = AppendValue(dst, p.Cols[i], row[p.Src[i]]); err != nil {
			return dst, err
		}
	}
	return dst, nil
}

// Hash is the row hash checksums are built from: two FNV-1a 32-bit lanes
// with different offsets. Not cryptographic; checksums detect drift, they
// do not defend against an adversary.
func Hash(b []byte) uint64 {
	h1, h2 := uint32(0x811c9dc5), uint32(0x9e3779b9)
	for _, c := range b {
		h1 = (h1 ^ uint32(c)) * 16777619
		h2 = (h2 ^ uint32(c)) * 16777619
	}
	return uint64(h2)<<32 | uint64(h1)
}
