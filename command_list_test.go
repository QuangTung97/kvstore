package kvstore

import (
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
	"unsafe"
)

func newCommandListStore() *commandListStore {
	s := &commandListStore{}
	initCommandListStore(s, 1024)
	return s
}

func TestCommandListHeader(t *testing.T) {
	var headerData [commandListHeaderSize / 8]uint64
	headerData[0] = 0x1234567801020304

	data := getByteArrays(headerData[:])
	assert.Equal(t, 8, len(data))
	assert.Equal(t, []byte{0x04, 0x03, 0x02, 0x01, 0x78, 0x56, 0x34, 0x12}, data)

	assert.Equal(t, uint64(8), commandListHeaderSize)
	assert.Equal(t, uint64(1), commandListHeaderSizeUint64)
	assert.Equal(t, uintptr(2), unsafe.Alignof(commandListHeader{}))
	assert.Equal(t, uintptr(8), unsafe.Alignof(headerData))
	assert.Equal(t, uintptr(8), unsafe.Sizeof(headerData))
}

func TestCommandListStore_AppendCommands_Single(t *testing.T) {
	p := newCommandListStore()
	p.appendCommands(net.IPv4(192, 168, 0, 1), 8100, []byte("some-data"))

	cmdList, _ := p.getNextRawCommandList()
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
