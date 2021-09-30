package kvstore

import (
	"net"
	"sync"
	"sync/atomic"
	"unsafe"
)

type commandListStore struct {
	mut  sync.Mutex
	cond *sync.Cond

	buffer     []byte
	nextOffset uint64
	processed  uint64

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
	begin := s.nextOffset
	copy(s.buffer[begin:], data)
	s.nextOffset += uint64(len(data))
}

func (s *commandListStore) readAt(data []byte, pos uint64) {
	copy(data, s.buffer[pos:])
}

func (s *commandListStore) appendCommands(ip net.IP, port uint16, data []byte) {
	s.mut.Lock()

	length := uint16(len(data))

	var headerData [commandListHeaderSize]byte
	header := (*commandListHeader)(unsafe.Pointer(&headerData[0]))
	copy(header.ip[:], ip.To4())
	header.port = port
	header.length = length

	s.appendBytes(headerData[:])
	s.appendBytes(data)

	s.mut.Unlock()
	s.cond.Signal()
}

func (s *commandListStore) getNextRawCommandList() (rawCommandList, uint64) {
	begin := s.processed

	var headerData [commandListHeaderSize]byte
	s.readAt(headerData[:], begin)
	header := (*commandListHeader)(unsafe.Pointer(&headerData[0]))

	s.readAt(s.currentCommandData[:header.length], begin+commandListHeaderSize)

	return rawCommandList{
		ip:   header.ip[:],
		port: header.port,
		data: s.currentCommandData[:header.length],
	}, begin + commandListHeaderSize + uint64(header.length)
}

func (s *commandListStore) commitProcessedOffset(value uint64) {
	atomic.StoreUint64(&s.processed, value)
}

func (s *commandListStore) getCommitProcessed() uint64 {
	return atomic.LoadUint64(&s.processed)
}
