package kvstore

import (
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func newCommandListStore() *commandListStore {
	s := &commandListStore{}
	initCommandListStore(s, 1024)
	return s
}

func TestCommandListStore_AppendCommands_Single(t *testing.T) {
	p := newCommandListStore()
	assert.Equal(t, uint64(0), p.nextOffset)
	p.appendCommands(net.IPv4(192, 168, 0, 1), 8100, []byte("some-data"))

	cmdList, completedOffset := p.getNextRawCommandList()
	assert.Equal(t, uint64(dataOffset+len("some-data")), completedOffset)
	assert.Equal(t, rawCommandList{
		ip:   net.IPv4(192, 168, 0, 1).To4(),
		port: 8100,
		data: []byte("some-data"),
	}, cmdList)
}

func TestCommandListStore_AppendCommands_Multiple(t *testing.T) {
	p := newCommandListStore()

	assert.Equal(t, uint64(0), p.nextOffset)

	p.appendCommands(net.IPv4(192, 168, 0, 1), 8100, []byte("some-data"))
	p.appendCommands(net.IPv4(123, 9, 2, 5), 7233, []byte("another-data"))
	p.appendCommands(net.IPv4(89, 0, 3, 6), 7000, []byte("random-data"))

	cmdList, completedOffset := p.getNextRawCommandList()
	assert.Equal(t, rawCommandList{
		ip:   net.IPv4(192, 168, 0, 1).To4(),
		port: 8100,
		data: []byte("some-data"),
	}, cmdList)

	p.commitProcessedOffset(completedOffset)

	cmdList, completedOffset = p.getNextRawCommandList()
	assert.Equal(t, rawCommandList{
		ip:   net.IPv4(123, 9, 2, 5).To4(),
		port: 7233,
		data: []byte("another-data"),
	}, cmdList)

	p.commitProcessedOffset(completedOffset)

	cmdList, completedOffset = p.getNextRawCommandList()
	assert.Equal(t, rawCommandList{
		ip:   net.IPv4(89, 0, 3, 6).To4(),
		port: 7000,
		data: []byte("random-data"),
	}, cmdList)

	p.commitProcessedOffset(completedOffset)
	assert.Equal(t, completedOffset, p.getCommitProcessed())
}
