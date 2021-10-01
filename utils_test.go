package kvstore

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestParseHeader_Without_Fragmented(t *testing.T) {
	data := []byte{
		0x12, 0, 0, 0,
		0, 0, 0, 0,
	}
	result, offset := parseDataFrameHeader(data)
	assert.Equal(t, 8, offset)
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
	result, offset := parseDataFrameHeader(data)
	assert.Equal(t, 16, offset)
	assert.Equal(t, dataFrameHeader{
		batchID:    0x22,
		fragmented: true,
		length:     0x15,
		offset:     0x07,
	}, result)
}

func TestParseHeader_Error_Missing_Batch_ID_Data(t *testing.T) {
	data := []byte{
		0x22, 0, 0,
	}
	result, offset := parseDataFrameHeader(data)
	assert.Equal(t, 0, offset)
	assert.Equal(t, dataFrameHeader{}, result)
}

func TestParseHeader_Error_Missing_Length_Data(t *testing.T) {
	data := []byte{
		0x22, 0, 0, 0,
		0, 0, 0, 0x80,
	}
	result, offset := parseDataFrameHeader(data)
	assert.Equal(t, 0, offset)
	assert.Equal(t, dataFrameHeader{}, result)
}

func TestParseHeader_Error_Missing_Offset_Data(t *testing.T) {
	data := []byte{
		0x22, 0, 0, 0,
		0, 0, 0, 0x80,
		0x15, 0, 0, 0,
		0x07, 0, 0,
	}
	result, offset := parseDataFrameHeader(data)
	assert.Equal(t, 0, offset)
	assert.Equal(t, dataFrameHeader{}, result)
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

func TestBuildDataFrameEntry(t *testing.T) {
	data := make([]byte, 8)
	nextOffset := buildDataFrameEntry(data, []byte("abcd123"))
	assert.Equal(t, byte(7), data[0])
	assert.Equal(t, []byte("abcd123"), data[1:])
	assert.Equal(t, 8, nextOffset)
}

func TestParseDataFrameEntry(t *testing.T) {
	data := make([]byte, 1000)
	buildDataFrameEntry(data, []byte(strings.Repeat("A", 600)))
	content, nextOffset := parseDataFrameEntry(data)
	assert.Equal(t, []byte(strings.Repeat("A", 600)), content)
	assert.Equal(t, 602, nextOffset)
}

func TestParseDataFrameEntry_Error_While_Read_VarUint(t *testing.T) {
	data := []byte{0xfa}
	content, nextOffset := parseDataFrameEntry(data)
	assert.Equal(t, []byte(nil), content)
	assert.Equal(t, 0, nextOffset)
}

func TestParseDataFrameEntry_Error_Missing_Data(t *testing.T) {
	data := []byte{0x08, 0x02}
	content, nextOffset := parseDataFrameEntry(data)
	assert.Equal(t, []byte(nil), content)
	assert.Equal(t, 0, nextOffset)
}
