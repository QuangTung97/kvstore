package kvstore

import (
	"encoding/binary"
	"errors"
	"github.com/QuangTung97/kvstore/kvstorepb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseDataFrameHeader(t *testing.T) {
	data := make([]byte, 12)
	binary.LittleEndian.PutUint32(data, 30)
	binary.LittleEndian.PutUint32(data[4:], 100)
	binary.LittleEndian.PutUint32(data[8:], 50)

	header := parseDataFrameHeader(data)
	assert.Equal(t, dataFrameHeader{
		batchID: 30,
		length:  100,
		offset:  50,
	}, header)
}

func TestParseRawCommandList_Single(t *testing.T) {
	data := make([]byte, 50)
	cmdGet := kvstorepb.Command{
		Type: kvstorepb.CommandType_COMMAND_TYPE_LEASE_GET,
		Id:   60,
		LeaseGet: &kvstorepb.CommandLeaseGet{
			Key: "some-key",
		},
	}

	size := cmdGet.Size()
	offset := binary.PutUvarint(data, uint64(size))
	_, err := cmdGet.MarshalToSizedBuffer(data[offset : offset+size])
	assert.Equal(t, nil, err)

	cmdList, err := parseRawCommandList(data[:offset+size])
	assert.Equal(t, nil, err)
	assert.Equal(t, []*kvstorepb.Command{
		{
			Type: kvstorepb.CommandType_COMMAND_TYPE_LEASE_GET,
			Id:   60,
			LeaseGet: &kvstorepb.CommandLeaseGet{
				Key: "some-key",
			},
		},
	}, cmdList)
}

func TestParseRawCommandList_Multiple(t *testing.T) {
	data := make([]byte, 200)
	cmdGet := kvstorepb.Command{
		Type: kvstorepb.CommandType_COMMAND_TYPE_LEASE_GET,
		Id:   60,
		LeaseGet: &kvstorepb.CommandLeaseGet{
			Key: "some-key",
		},
	}
	cmdSet := kvstorepb.Command{
		Type: kvstorepb.CommandType_COMMAND_TYPE_LEASE_SET,
		Id:   61,
		LeaseSet: &kvstorepb.CommandLeaseSet{
			Key:     "another-key",
			LeaseId: 100,
			Value:   "some-value",
		},
	}

	size := cmdGet.Size()
	offset := binary.PutUvarint(data, uint64(size))
	_, err := cmdGet.MarshalToSizedBuffer(data[offset : offset+size])
	assert.Equal(t, nil, err)

	offset += size

	size = cmdSet.Size()
	offset += binary.PutUvarint(data[offset:], uint64(size))
	_, err = cmdSet.MarshalToSizedBuffer(data[offset : offset+size])
	assert.Equal(t, nil, err)

	cmdList, err := parseRawCommandList(data[:offset+size])
	assert.Equal(t, nil, err)
	assert.Equal(t, []*kvstorepb.Command{
		{
			Type: kvstorepb.CommandType_COMMAND_TYPE_LEASE_GET,
			Id:   60,
			LeaseGet: &kvstorepb.CommandLeaseGet{
				Key: "some-key",
			},
		},
		{
			Type: kvstorepb.CommandType_COMMAND_TYPE_LEASE_SET,
			Id:   61,
			LeaseSet: &kvstorepb.CommandLeaseSet{
				Key:     "another-key",
				LeaseId: 100,
				Value:   "some-value",
			},
		},
	}, cmdList)
}

func TestParseRawCommandList_Error_Parse_Size(t *testing.T) {
	_, err := parseRawCommandList([]byte{128 + 2})
	assert.Equal(t, errors.New("invalid command size"), err)
}

func TestParseRawCommandList_Error_Cmd_Data_Size(t *testing.T) {
	_, err := parseRawCommandList([]byte{15})
	assert.Equal(t, errors.New("invalid command data size"), err)
}

func TestParseRawCommandList_Error_Unmarshal_Error(t *testing.T) {
	_, err := parseRawCommandList([]byte{2, 0, 0})
	assert.Error(t, err)
}
