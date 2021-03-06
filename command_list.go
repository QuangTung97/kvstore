package kvstore

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

type atomicUint64 struct {
	value uint64
}

// IPAddr only supported IPv4
type IPAddr [4]byte

type rawCommandList struct {
	ip   IPAddr
	port uint16
	data []byte
}

func (a *atomicUint64) store(v uint64) {
	atomic.StoreUint64(&a.value, v)
}

func (a *atomicUint64) load() uint64 {
	return atomic.LoadUint64(&a.value)
}

type commandListStore struct {
	mut     sync.Mutex
	cond    *sync.Cond
	stopped bool

	buffer     []byte
	nextOffset uint64
	processed  atomicUint64

	currentCommandData []byte
}

type commandListHeader struct {
	ip     [4]byte
	port   uint16
	length uint16
}

const commandListHeaderSize = uint64(unsafe.Sizeof(commandListHeader{}))

func initCommandListStore(s *commandListStore, bufSize int) {
	s.buffer = make([]byte, bufSize)
	s.currentCommandData = make([]byte, 1<<16) // 64KB
	s.cond = sync.NewCond(&s.mut)
}

func (s *commandListStore) appendBytes(data []byte) {
	max := len(s.buffer)
	index := int(s.nextOffset) % max
	copy(s.buffer[index:], data)
	if index+len(data) > max {
		firstPart := max - index
		secondPart := len(data) - firstPart
		copy(s.buffer[:secondPart], data[firstPart:])
	}
	s.nextOffset += uint64(len(data))
}

func (s *commandListStore) readAt(data []byte, pos uint64) {
	max := len(s.buffer)
	index := int(pos) % max
	copy(data, s.buffer[index:])
	if index+len(data) > max {
		firstPart := max - index
		copy(data[firstPart:], s.buffer)
	}
}

func (s *commandListStore) appendCommands(ip IPAddr, port uint16, data []byte) {
	s.mut.Lock()

	length := uint16(len(data))

	var headerData [commandListHeaderSize]byte
	header := (*commandListHeader)(unsafe.Pointer(&headerData[0]))
	header.ip = ip
	header.port = port
	header.length = length

	s.appendBytes(headerData[:])
	s.appendBytes(data)

	s.mut.Unlock()
	s.cond.Signal()
}

func (s *commandListStore) getNextRawCommandList() (rawCommandList, uint64) {
	begin := s.processed.load()

	var headerData [commandListHeaderSize]byte
	s.readAt(headerData[:], begin)
	header := (*commandListHeader)(unsafe.Pointer(&headerData[0]))

	s.readAt(s.currentCommandData[:header.length], begin+commandListHeaderSize)

	return rawCommandList{
		ip:   header.ip,
		port: header.port,
		data: s.currentCommandData[:header.length],
	}, begin + commandListHeaderSize + uint64(header.length)
}

func (s *commandListStore) commitProcessedOffset(value uint64) {
	s.processed.store(value)
}

func (s *commandListStore) getCommitProcessed() uint64 {
	return s.processed.load()
}

// when stopped, return false
func (s *commandListStore) waitAvailable() bool {
	s.mut.Lock()
	for s.nextOffset <= s.processed.load() && !s.stopped {
		s.cond.Wait()
	}
	continued := s.nextOffset > s.processed.load() || !s.stopped
	s.mut.Unlock()
	return continued
}

func (s *commandListStore) isCommandAppendable(dataSize int) bool {
	max := uint64(len(s.buffer))
	sizeWithHeader := uint64(dataSize) + commandListHeaderSize
	return max+s.processed.load() >= s.nextOffset+sizeWithHeader
}

func (s *commandListStore) stopWait() {
	s.mut.Lock()
	s.stopped = true
	s.mut.Unlock()

	s.cond.Signal()
}
