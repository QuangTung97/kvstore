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

func parseDataFrameHeader(data []byte) (dataFrameHeader, int, error) {
	batchID := binary.LittleEndian.Uint64(data)
	if batchID&fragmentedBitMask == 0 {
		return dataFrameHeader{
			batchID:    batchID,
			fragmented: false,
		}, 0, nil
	}

	length := binary.LittleEndian.Uint32(data[dataFrameLengthOffset:])
	offset := binary.LittleEndian.Uint32(data[dataFrameOffsetFieldOffset:])

	return dataFrameHeader{
		batchID:    batchID & batchIDMask,
		fragmented: true,
		length:     length,
		offset:     offset,
	}, 0, nil
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
