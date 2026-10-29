package wire

import (
	"encoding/binary"
	"errors"

	"github.com/nightridefm/localsync/internal/catalog"
)

// Column-wise row hashing and PATCH ops.
//
// A row's hash is the sum (mod 2^64) of its column hashes, so the engine can
// keep one hash per projected column per member row and tell exactly which
// columns an update changed without holding old rows or their values (which
// REPLICA IDENTITY DEFAULT does not send, and unchanged TOAST columns never
// are). The client recomputes the same sum from PUT bytes, and takes it from
// PATCH ops, which carry it.
//
//	patch = u64 LE row hash | uvarint len pk | changed bitmap | null bitmap | value*
//
// Both bitmaps have one bit per projected column; values are the changed,
// non-NULL columns in column order.

const OpPatch = 3

// ColumnHash hashes column i's encoded value (nil for NULL).
func ColumnHash(i int, v []byte) uint64 {
	h1, h2 := uint32(0x811c9dc5), uint32(0x9e3779b9)
	mix := func(c byte) {
		h1 = (h1 ^ uint32(c)) * 16777619
		h2 = (h2 ^ uint32(c)) * 16777619
	}
	mix(byte(i))
	mix(byte(i >> 8))
	if v == nil {
		mix(0)
	} else {
		mix(1)
		for _, c := range v {
			mix(c)
		}
	}
	return uint64(h2)<<32 | uint64(h1)
}

// EncodeColumns encodes each projected column of row (table column order);
// nil entries are NULL. unknown marks table columns whose value was not
// sent (unchanged TOAST); their entries are nil and their known[] false.
func (p *Projection) EncodeColumns(row [][]byte, unknown []bool) (vals [][]byte, known []bool, err error) {
	vals = make([][]byte, len(p.Cols))
	known = make([]bool, len(p.Cols))
	for i, src := range p.Src {
		if unknown != nil && src < len(unknown) && unknown[src] {
			continue
		}
		known[i] = true
		if row[src] == nil {
			continue
		}
		if vals[i], err = AppendValue(nil, p.Cols[i], row[src]); err != nil {
			return nil, nil, err
		}
	}
	return vals, known, nil
}

// BuildRow assembles a full row from per-column encoded values.
func BuildRow(n int, vals [][]byte) []byte {
	nb := (n + 7) / 8
	out := make([]byte, nb, nb+64)
	for i, v := range vals {
		if v == nil {
			out[i/8] |= 1 << (i % 8)
			continue
		}
		out = append(out, v...)
	}
	return out
}

// SplitRow is the inverse of BuildRow: per-column encoded values.
func SplitRow(cols []Column, row []byte) ([][]byte, error) {
	nb := (len(cols) + 7) / 8
	if len(row) < nb {
		return nil, errors.New("short row")
	}
	vals := make([][]byte, len(cols))
	pos := nb
	for i, c := range cols {
		if row[i/8]&(1<<(i%8)) != 0 {
			continue
		}
		n, err := valueLen(c, row[pos:])
		if err != nil {
			return nil, err
		}
		vals[i] = row[pos : pos+n]
		pos += n
	}
	return vals, nil
}

// valueLen is the encoded length of one value at the start of b.
func valueLen(c Column, b []byte) (int, error) {
	switch c.Kind {
	case catalog.Int, catalog.Timestamp:
		_, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, errors.New("bad varint")
		}
		return n, nil
	case catalog.Float:
		return 8, nil
	case catalog.Bool:
		return 1, nil
	case catalog.UUID:
		return 16, nil
	}
	l, n := binary.Uvarint(b)
	if n <= 0 || uint64(len(b)-n) < l {
		return 0, errors.New("bad length")
	}
	return n + int(l), nil
}

// Patch is a decoded PATCH op.
type Patch struct {
	Hash    uint64
	PK      []byte
	Changed []bool
	Vals    [][]byte // per column; meaningful where Changed
}

func AppendPatch(dst []byte, n int, p *Patch) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, p.Hash)
	dst = binary.AppendUvarint(dst, uint64(len(p.PK)))
	dst = append(dst, p.PK...)
	nb := (n + 7) / 8
	ch := len(dst)
	for range 2 * nb {
		dst = append(dst, 0)
	}
	for i := range n {
		if !p.Changed[i] {
			continue
		}
		dst[ch+i/8] |= 1 << (i % 8)
		if p.Vals[i] == nil {
			dst[ch+nb+i/8] |= 1 << (i % 8)
			continue
		}
		dst = append(dst, p.Vals[i]...)
	}
	return dst
}

func DecodePatch(cols []Column, b []byte) (*Patch, error) {
	n := len(cols)
	nb := (n + 7) / 8
	if len(b) < 8 {
		return nil, errors.New("short patch")
	}
	p := &Patch{Hash: binary.LittleEndian.Uint64(b), Changed: make([]bool, n), Vals: make([][]byte, n)}
	l, k := binary.Uvarint(b[8:])
	if k <= 0 {
		return nil, errors.New("bad patch pk")
	}
	pos := 8 + k
	p.PK = b[pos : pos+int(l)]
	pos += int(l)
	if len(b) < pos+2*nb {
		return nil, errors.New("short patch bitmaps")
	}
	chg, nul := b[pos:pos+nb], b[pos+nb:pos+2*nb]
	pos += 2 * nb
	for i, c := range cols {
		if chg[i/8]&(1<<(i%8)) == 0 {
			continue
		}
		p.Changed[i] = true
		if nul[i/8]&(1<<(i%8)) != 0 {
			continue
		}
		m, err := valueLen(c, b[pos:])
		if err != nil {
			return nil, err
		}
		p.Vals[i] = b[pos : pos+m]
		pos += m
	}
	return p, nil
}

// ApplyPatch applies a patch to a full encoded row.
func ApplyPatch(cols []Column, row []byte, p *Patch) ([]byte, error) {
	vals, err := SplitRow(cols, row)
	if err != nil {
		return nil, err
	}
	for i := range cols {
		if p.Changed[i] {
			vals[i] = p.Vals[i]
		}
	}
	return BuildRow(len(cols), vals), nil
}

// MergePatches folds b (later) into a (earlier) for the same row.
func MergePatches(a, b *Patch) *Patch {
	out := &Patch{Hash: b.Hash, PK: b.PK, Changed: make([]bool, len(a.Changed)), Vals: make([][]byte, len(a.Vals))}
	for i := range a.Changed {
		switch {
		case b.Changed[i]:
			out.Changed[i], out.Vals[i] = true, b.Vals[i]
		case a.Changed[i]:
			out.Changed[i], out.Vals[i] = true, a.Vals[i]
		}
	}
	return out
}

// RowHash is the hash of a full encoded row: the sum of its column hashes.
// MemberHash is a row's contribution to its unit's checksum: the row hash
// through the splitmix64 finalizer (a bijection). Row hashes are sums of
// column hashes, so summing them directly would make the unit checksum a
// sum over (column, value) pairs, blind to values moving between rows (two
// projects swapping orgs); mixing per row breaks that linearity.
func MemberHash(rowHash uint64) uint64 {
	z := rowHash
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func RowHash(cols []Column, row []byte) (uint64, error) {
	vals, err := SplitRow(cols, row)
	if err != nil {
		return 0, err
	}
	var h uint64
	for i, v := range vals {
		h += ColumnHash(i, v)
	}
	return h, nil
}

// AppendRowHashed encodes row like AppendRow and also writes each column's
// hash into hashes (len(p.Cols)), in one pass over the values.
func (p *Projection) AppendRowHashed(dst []byte, row [][]byte, hashes []uint64) ([]byte, error) {
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
			hashes[i] = ColumnHash(i, nil)
			continue
		}
		at := len(dst)
		if dst, err = AppendValue(dst, p.Cols[i], v); err != nil {
			return dst, err
		}
		hashes[i] = ColumnHash(i, dst[at:])
	}
	return dst, nil
}
