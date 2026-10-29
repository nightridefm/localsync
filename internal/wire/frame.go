package wire

import "encoding/binary"

// Frame layout (one WebSocket binary message):
//
//	u8 1 (or 2: part, more follow) | uvarint lsn | uvarint units | unit*
//	unit = uvarint ref | u8 flags (bit0 RESET, bit1 continues) | u64 LE checksum after | uvarint ops | op*
//	op   = u8 1 (PUT)    uvarint len row
//	     | u8 2 (REMOVE) uvarint len pk
const (
	MsgFrame = 1
	// MsgFramePart: a part of a frame; more parts follow and the frame ends
	// with a MsgFrame. The client applies all parts in one transaction.
	MsgFramePart = 2
	OpPut        = 1
	OpRemove     = 2
	FlagReset    = 1
	// FlagContinues: more ops for this unit follow in a later part; its
	// checksum is only meaningful on the last piece.
	FlagContinues = 2
)

type Op struct {
	Kind  byte
	Bytes []byte // row for PUT, pk for REMOVE
}

func AppendFrameHeader(dst []byte, lsn uint64, units int) []byte {
	return AppendPartHeader(dst, MsgFrame, lsn, units)
}

func AppendPartHeader(dst []byte, msg byte, lsn uint64, units int) []byte {
	dst = append(dst, msg)
	dst = binary.AppendUvarint(dst, lsn)
	return binary.AppendUvarint(dst, uint64(units))
}

func AppendUnit(dst []byte, ref uint32, reset bool, checksum uint64, ops []Op) []byte {
	return AppendUnitPart(dst, ref, reset, false, checksum, ops)
}

func AppendUnitPart(dst []byte, ref uint32, reset, continues bool, checksum uint64, ops []Op) []byte {
	dst = binary.AppendUvarint(dst, uint64(ref))
	var flags byte
	if reset {
		flags |= FlagReset
	}
	if continues {
		flags |= FlagContinues
	}
	dst = append(dst, flags)
	dst = binary.LittleEndian.AppendUint64(dst, checksum)
	dst = binary.AppendUvarint(dst, uint64(len(ops)))
	for _, op := range ops {
		dst = append(dst, op.Kind)
		dst = binary.AppendUvarint(dst, uint64(len(op.Bytes)))
		dst = append(dst, op.Bytes...)
	}
	return dst
}
