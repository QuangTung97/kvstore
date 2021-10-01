package kvstore

import (
	"encoding/binary"
)

type dataFrameHeader struct {
	batchID    uint64
	fragmented bool
	length     uint32
	offset     uint32
}

const fragmentedBitMask uint64 = 1 << 63
const batchIDMask = ^fragmentedBitMask

const dataFrameLengthOffset = 8
const dataFrameOffsetFieldOffset = dataFrameLengthOffset + 4
const dataFrameEntryListOffset = dataFrameOffsetFieldOffset + 4

// nextOffset will be zero if error occurs
func parseDataFrameHeader(data []byte) (header dataFrameHeader, nextOffset int) {
	if len(data) < dataFrameLengthOffset {
		return dataFrameHeader{}, 0
	}

	batchID := binary.LittleEndian.Uint64(data)
	if batchID&fragmentedBitMask == 0 {
		return dataFrameHeader{
			batchID:    batchID,
			fragmented: false,
		}, dataFrameLengthOffset
	}

	if len(data) < dataFrameEntryListOffset {
		return dataFrameHeader{}, 0
	}

	length := binary.LittleEndian.Uint32(data[dataFrameLengthOffset:])
	offset := binary.LittleEndian.Uint32(data[dataFrameOffsetFieldOffset:])

	return dataFrameHeader{
		batchID:    batchID & batchIDMask,
		fragmented: true,
		length:     length,
		offset:     offset,
	}, dataFrameEntryListOffset
}

func buildDataFrameHeader(data []byte, header dataFrameHeader) {
	if !header.fragmented {
		binary.LittleEndian.PutUint64(data, header.batchID)
		return
	}
	batchID := header.batchID | fragmentedBitMask
	binary.LittleEndian.PutUint64(data, batchID)
	binary.LittleEndian.PutUint32(data[dataFrameLengthOffset:], header.length)
	binary.LittleEndian.PutUint32(data[dataFrameOffsetFieldOffset:], header.offset)
}

func buildDataFrameEntry(dest []byte, data []byte) int {
	dataLen := len(data)
	size := binary.PutUvarint(dest, uint64(dataLen))
	copy(dest[size:], data)
	return size + dataLen
}

// return nil, 0 when error occurs
func parseDataFrameEntry(data []byte) ([]byte, int) {
	readLen, offset := binary.Uvarint(data)
	if readLen <= 0 {
		return nil, 0
	}
	dataLen := int(readLen)
	if offset+dataLen > len(data) {
		return nil, 0
	}
	return data[offset : offset+dataLen], offset + dataLen
}
