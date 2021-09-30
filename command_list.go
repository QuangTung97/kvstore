package kvstore

import (
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
)

type commandListStore struct {
	mut  sync.Mutex
	cond *sync.Cond

	buffer     []byte
	nextOffset uint64
	processed  uint64
}

func initCommandListStore(s *commandListStore, bufSize int) {
	s.buffer = make([]byte, bufSize)
	s.cond = sync.NewCond(&s.mut)
}

func (s *commandListStore) computeSlice(n uint16) []byte {
	begin := s.nextOffset
	end := begin + dataOffset + uint64(n)
	return s.buffer[begin:end]
}

func (s *commandListStore) appendCommands(ip net.IP, port uint16, data []byte) {
	s.mut.Lock()

	length := uint16(len(data))

	slice := s.computeSlice(length)
	copy(slice, ip.To4())
	binary.LittleEndian.PutUint16(slice[portOffset:], port)
	binary.LittleEndian.PutUint16(slice[lengthOffset:], length)
	copy(slice[dataOffset:], data)
	s.nextOffset += uint64(len(slice))

	s.mut.Unlock()
	s.cond.Signal()
}

func (s *commandListStore) getNextRawCommandList() (rawCommandList, uint64) {
	begin := s.processed

	slice := s.buffer[begin:]

	ip := slice[:net.IPv4len]
	port := binary.LittleEndian.Uint16(slice[portOffset : portOffset+2])
	length := binary.LittleEndian.Uint16(slice[lengthOffset : lengthOffset+2])
	data := slice[dataOffset : dataOffset+uint64(length)]

	return rawCommandList{
		ip:   ip,
		port: port,
		data: data,
	}, begin + dataOffset + uint64(length)
}

func (s *commandListStore) commitProcessedOffset(value uint64) {
	atomic.StoreUint64(&s.processed, value)
}

func (s *commandListStore) getCommitProcessed() uint64 {
	return atomic.LoadUint64(&s.processed)
}
