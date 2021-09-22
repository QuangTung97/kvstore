package kvstore

import (
	"encoding/binary"
	"errors"
	"github.com/QuangTung97/kvstore/kvstorepb"
)

type dataFrameHeader struct {
	batchID uint32
	length  uint32
	offset  uint32
}

const dataFrameLengthOffset = 4
const dataFrameOffsetFieldOffset = 8
const dataFrameEntryListOffset = 12

func parseDataFrameHeader(data []byte) dataFrameHeader {
	batchID := binary.LittleEndian.Uint32(data)
	length := binary.LittleEndian.Uint32(data[dataFrameLengthOffset:])
	offset := binary.LittleEndian.Uint32(data[dataFrameOffsetFieldOffset:])

	return dataFrameHeader{
		batchID: batchID,
		length:  length,
		offset:  offset,
	}
}

func parseEntryList(data []byte, unmarshal func(data []byte) error) error {
	for len(data) > 0 {
		size, offset := binary.Uvarint(data)
		if offset <= 0 {
			return errors.New("invalid command size")
		}

		if len(data) < offset+int(size) {
			return errors.New("invalid command data size")
		}

		err := unmarshal(data[offset : offset+int(size)])
		if err != nil {
			return err
		}
		data = data[uint64(offset)+size:]
	}
	return nil
}

func parseRawCommandList(data []byte) ([]*kvstorepb.Command, error) {
	var result []*kvstorepb.Command
	err := parseEntryList(data, func(data []byte) error {
		cmd := &kvstorepb.Command{}
		err := cmd.Unmarshal(data)
		if err != nil {
			return err
		}
		result = append(result, cmd)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func parseCommandResultList(data []byte) ([]*kvstorepb.CommandResult, error) {
	var result []*kvstorepb.CommandResult
	err := parseEntryList(data, func(data []byte) error {
		cmdResult := &kvstorepb.CommandResult{}
		err := cmdResult.Unmarshal(data)
		if err != nil {
			return err
		}
		result = append(result, cmdResult)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func buildLeaseGetCmd(id uint64, key string) *kvstorepb.Command {
	return &kvstorepb.Command{
		Type: kvstorepb.CommandType_COMMAND_TYPE_LEASE_GET,
		Id:   id,
		LeaseGet: &kvstorepb.CommandLeaseGet{
			Key: key,
		},
	}
}

func buildLeaseSetCmd(id uint64, key string, lease uint64, value string) *kvstorepb.Command {
	return &kvstorepb.Command{
		Type: kvstorepb.CommandType_COMMAND_TYPE_LEASE_SET,
		Id:   id,
		LeaseSet: &kvstorepb.CommandLeaseSet{
			Key:     key,
			LeaseId: lease,
			Value:   value,
		},
	}
}

func buildDataFrameHeader(data []byte, header dataFrameHeader) {
	binary.LittleEndian.PutUint32(data, header.batchID)
	binary.LittleEndian.PutUint32(data[dataFrameLengthOffset:], header.length)
	binary.LittleEndian.PutUint32(data[dataFrameOffsetFieldOffset:], header.offset)
}

func buildRawCommandList(data []byte, cmdList ...*kvstorepb.Command) int {
	total := 0
	for _, cmd := range cmdList {
		size := cmd.Size()
		offset := binary.PutUvarint(data, uint64(size))
		_, err := cmd.MarshalToSizedBuffer(data[offset : offset+size])
		if err != nil {
			panic(err)
		}
		total += offset + size
		data = data[offset+size:]
	}
	return total
}

func buildRawCommandListBatch(batchID uint32, cmdList ...*kvstorepb.Command) []byte {
	data := make([]byte, 2048)

	total := buildRawCommandList(data[dataFrameEntryListOffset:], cmdList...)
	buildDataFrameHeader(data, dataFrameHeader{
		batchID: batchID,
		length:  uint32(total),
		offset:  0,
	})

	return data[:dataFrameEntryListOffset+total]
}
