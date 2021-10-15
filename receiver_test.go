package kvstore

import (
	"github.com/QuangTung97/kvstore/lease"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
)

func newReceiver(sender ResponseSender, options ...Option) *receiver {
	r := &receiver{}

	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	options = append(options, WithLogger(logger))

	opts := computeOptions(options...)

	cache := lease.New(4, 1<<16)
	initReceiver(r, cache, sender, opts)
	return r
}

func TestReceiver_Not_Fragmented(t *testing.T) {
	sender := &ResponseSenderMock{}
	r := newReceiver(sender)

	r.runInBackground()

	data := make([]byte, 1000)
	offset := buildDataFrameHeader(data, dataFrameHeader{
		batchID:    10,
		fragmented: false,
	})
	cmd := "LGET key01\r\n"
	buildDataFrameEntryHeader(data[offset:], 50, len(cmd))
	offset += entryDataOffset
	copy(data[offset:], cmd)
	offset += len(cmd)

	var sendDataList [][]byte
	sender.SendFunc = func(ip IPAddr, port uint16, data []byte) error {
		sendDataList = append(sendDataList, cloneBytes(data))
		return nil
	}

	r.recv(newIPAddr(192, 168, 10, 12), 7200, data[:offset])

	r.shutdown()

	assert.Equal(t, 1, len(sender.SendCalls()))
	assert.Equal(t, newIPAddr(192, 168, 10, 12), sender.SendCalls()[0].IP)
	assert.Equal(t, uint16(7200), sender.SendCalls()[0].Port)

	sendData := checkAndGetSendData(t, sendDataList[0], 1)
	requestID, content, nextOffset := parseDataFrameEntry(sendData)
	assert.Equal(t, uint64(50), requestID)
	assert.Equal(t, "GRANTED 1\r\n", string(content))
	assert.Equal(t, len(sendData), nextOffset)
}
