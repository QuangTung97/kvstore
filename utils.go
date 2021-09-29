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

const dataFrameLengthOffset = 4
const dataFrameOffsetFieldOffset = 8
const dataFrameEntryListOffset = 12

func parseDataFrameHeader(data []byte) dataFrameHeader {
	batchID := binary.LittleEndian.Uint64(data)
	length := binary.LittleEndian.Uint32(data[dataFrameLengthOffset:])
	offset := binary.LittleEndian.Uint32(data[dataFrameOffsetFieldOffset:])

	return dataFrameHeader{
		batchID: batchID,
		length:  length,
		offset:  offset,
	}
}

func buildDataFrameHeader(data []byte, header dataFrameHeader) {
}
