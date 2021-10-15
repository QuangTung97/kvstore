package kvstore

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newCommandListStore() *commandListStore {
	s := &commandListStore{}
	initCommandListStore(s, 1024)
	return s
}

func newCommandListStoreBuffSize(buffSize int) *commandListStore {
	s := &commandListStore{}
	initCommandListStore(s, buffSize)
	return s
}

func newIPAddr(a, b, c, d byte) IPAddr {
	ip := net.IPv4(a, b, c, d).To4()
	var result IPAddr
	copy(result[:], ip)
	return result
}

func TestCommandListStore_AppendCommands_Single(t *testing.T) {
	s := newCommandListStore()
	s.appendCommands(newIPAddr(192, 168, 0, 1), 8100, []byte("some-data"))

	cmdList, _ := s.getNextRawCommandList()
	assert.Equal(t, rawCommandList{
		ip:   newIPAddr(192, 168, 0, 1),
		port: 8100,
		data: []byte("some-data"),
	}, cmdList)
}

func TestCommandListStore_AppendCommands_Multiple(t *testing.T) {
	s := newCommandListStore()

	s.appendCommands(newIPAddr(192, 168, 0, 1), 8100, []byte("some-data"))
	s.appendCommands(newIPAddr(123, 9, 2, 5), 7233, []byte("another-data"))
	s.appendCommands(newIPAddr(89, 0, 3, 6), 7000, []byte("random-data"))

	cmdList, completedOffset := s.getNextRawCommandList()
	assert.Equal(t, rawCommandList{
		ip:   newIPAddr(192, 168, 0, 1),
		port: 8100,
		data: []byte("some-data"),
	}, cmdList)

	s.commitProcessedOffset(completedOffset)

	cmdList, completedOffset = s.getNextRawCommandList()
	assert.Equal(t, rawCommandList{
		ip:   newIPAddr(123, 9, 2, 5),
		port: 7233,
		data: []byte("another-data"),
	}, cmdList)

	s.commitProcessedOffset(completedOffset)

	cmdList, completedOffset = s.getNextRawCommandList()
	assert.Equal(t, rawCommandList{
		ip:   newIPAddr(89, 0, 3, 6),
		port: 7000,
		data: []byte("random-data"),
	}, cmdList)

	s.commitProcessedOffset(completedOffset)
	assert.Equal(t, completedOffset, s.getCommitProcessed())
}

func TestCommandListStore_WaitAvailable_Single_Command(t *testing.T) {
	s := newCommandListStore()
	s.appendCommands(newIPAddr(192, 168, 0, 1), 8100, []byte("some-data"))
	continued := s.waitAvailable()
	assert.Equal(t, true, continued)
}

func TestCommandListStore_WaitAvailable_Stopped(t *testing.T) {
	s := newCommandListStore()
	s.stopWait()
	continued := s.waitAvailable()
	assert.Equal(t, false, continued)
}

func TestCommandListStore_WaitAvailable_No_Command(t *testing.T) {
	s := newCommandListStore()

	called := false
	go func() {
		s.waitAvailable()
		called = true
	}()
	time.Sleep(1 * time.Millisecond)

	assert.Equal(t, false, called)
}

func TestCommandListStore_Stress_Test(t *testing.T) {
	s := newCommandListStoreBuffSize(197)

	const numCommands = 1000
	count := uint32(0)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for s.waitAvailable() {
			cmdList, offset := s.getNextRawCommandList()

			assert.Equal(t, newIPAddr(198, 168, 53, 1), cmdList.ip)
			assert.Equal(t, uint16(8765), cmdList.port)
			assert.Equal(t, []byte("command-no-"), cmdList.data[:len("command-no-")])
			s.commitProcessedOffset(offset)

			atomic.AddUint32(&count, 1)
			time.Sleep(1 * time.Microsecond)
		}
	}()

	for i := 0; i < numCommands; i++ {
		data := []byte(fmt.Sprintf("command-no-%d", i))
		size := len(data)
		for !s.isCommandAppendable(size) {
			//revive:disable-next-line:empty-block
		}
		s.appendCommands(newIPAddr(198, 168, 53, 1), 8765, data)
	}

	for atomic.LoadUint32(&count) < numCommands {
		//revive:disable-next-line:empty-block
	}
	s.stopWait()
	wg.Wait()

	assert.Equal(t, uint32(numCommands), atomic.LoadUint32(&count))
}
