package kvstore

import (
	"github.com/QuangTung97/kvstore/kvstorepb"
	"github.com/QuangTung97/memtable"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func TestProcessor(t *testing.T) {
	p := newProcessor(1024, nil, nil)
	assert.Equal(t, uint64(0), p.nextOffset)
	p.appendCommands(net.IPv4(192, 168, 0, 1), 8100, []byte("some-data"))
	assert.Equal(t, uint64(20+len("some-data")), p.nextOffset)

	cmdList, nextProcessed := p.getNextRawCommandList()
	assert.Equal(t, rawCommandList{
		ip:   net.IPv4(192, 168, 0, 1),
		port: 8100,
		data: []byte("some-data"),
	}, cmdList)
	assert.Equal(t, 20+uint64(len("some-data")), nextProcessed)
}

// TODO append commands wrap around

func TestProcessor_RunSingleLoop(t *testing.T) {
	cache := memtable.New(10 << 20)
	sender := &ResponseSenderMock{}
	p := newProcessor(1024, cache, sender)

	sender.SendFunc = func(ip net.IP, port uint16, data []byte) error { return nil }

	p.appendCommands(
		net.IPv4(192, 168, 0, 1), 8100,
		buildRawCommandListBatch(80,
			buildLeaseGetCmd(11, "key01"),
			buildLeaseSetCmd(12, "key02", 200, "value02"),
		),
	)

	p.runSingleLoop()

	assert.Equal(t, 1, len(sender.SendCalls()))

	data := sender.SendCalls()[0].Data

	header := parseDataFrameHeader(data)
	assert.Equal(t, uint32(1), header.batchID)
	assert.Equal(t, uint32(len(data)-dataFrameEntryListOffset), header.length)
	assert.Equal(t, uint32(0), header.offset)

	cmdResults, err := parseCommandResultList(data[dataFrameEntryListOffset:])
	assert.Equal(t, nil, err)
	assert.Equal(t, []*kvstorepb.CommandResult{
		{
			Type: kvstorepb.CommandType_COMMAND_TYPE_LEASE_GET,
			Id:   11,
			LeaseGet: &kvstorepb.CommandLeaseGetResult{
				Status:  kvstorepb.LeaseGetStatus_LEASE_GET_STATUS_LEASE_GRANTED,
				LeaseId: 1,
			},
		},
		{
			Type: kvstorepb.CommandType_COMMAND_TYPE_LEASE_SET,
			Id:   12,
			LeaseSet: &kvstorepb.CommandLeaseSetResult{
				Affected: false,
			},
		},
	}, cmdResults)
}
