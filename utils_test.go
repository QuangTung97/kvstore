package kvstore

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseHeader_Without_Fragmented(t *testing.T) {
	data := []byte{
		0x12, 0, 0, 0,
		0, 0, 0, 0,
	}
	result, offset, err := parseDataFrameHeader(data)
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, offset)
	assert.Equal(t, dataFrameHeader{
		batchID:    0x12,
		fragmented: false,
	}, result)
}

func TestParseHeader_With_Fragmented(t *testing.T) {
	data := []byte{
		0x22, 0, 0, 0,
		0, 0, 0, 0x80,
		0x15, 0, 0, 0,
		0x07, 0, 0, 0,
	}
	result, offset, err := parseDataFrameHeader(data)
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, offset)
	assert.Equal(t, dataFrameHeader{
		batchID:    0x22,
		fragmented: true,
		length:     0x15,
		offset:     0x07,
	}, result)
}

func TestBuildDataFrameHeader_Not_Fragmented(t *testing.T) {
	data := make([]byte, dataFrameEntryListOffset)
	buildDataFrameHeader(data, dataFrameHeader{
		batchID:    0x28,
		fragmented: false,
		length:     0x17,
		offset:     0,
	})
	assert.Equal(t, []byte{
		0x28, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
	}, data)
}

func TestBuildDataFrameHeader_Fragmented(t *testing.T) {
	data := make([]byte, dataFrameEntryListOffset)
	buildDataFrameHeader(data, dataFrameHeader{
		batchID:    0x28,
		fragmented: true,
		length:     0x0258,
		offset:     0x36,
	})
	assert.Equal(t, []byte{
		0x28, 0, 0, 0,
		0, 0, 0, 0x80,
		0x58, 0x02, 0, 0,
		0x36, 0, 0, 0,
	}, data)
}
