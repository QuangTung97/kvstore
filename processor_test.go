package kvstore

import (
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
	assert.Equal(t, nil, sender.SendCalls()[0].Data)
}
