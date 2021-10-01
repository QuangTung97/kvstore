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

func buildDataFrameHeader(data []byte, header dataFrameHeader) int {
	if !header.fragmented {
		binary.LittleEndian.PutUint64(data, header.batchID)
		return dataFrameLengthOffset
	}
	batchID := header.batchID | fragmentedBitMask
	binary.LittleEndian.PutUint64(data, batchID)
	binary.LittleEndian.PutUint32(data[dataFrameLengthOffset:], header.length)
	binary.LittleEndian.PutUint32(data[dataFrameOffsetFieldOffset:], header.offset)
	return dataFrameEntryListOffset
}

const entryDataOffset = 12

func buildDataFrameEntryHeader(dest []byte, requestID uint64, dataSize int) {
	binary.LittleEndian.PutUint64(dest, requestID)
	binary.LittleEndian.PutUint32(dest[8:], uint32(dataSize))
}

// return nil, 0 when error occurs
func parseDataFrameEntry(data []byte) (uint64, []byte, int) {
	if len(data) < entryDataOffset {
		return 0, nil, 0
	}

	requestID := binary.LittleEndian.Uint64(data)
	dataLen := int(binary.LittleEndian.Uint32(data[8:]))
	if entryDataOffset+dataLen > len(data) {
		return 0, nil, 0
	}
	return requestID, data[entryDataOffset : entryDataOffset+dataLen], entryDataOffset + dataLen
}
