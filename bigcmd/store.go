package bigcmd

import (
	"unsafe"
)

// Store ...
type Store struct {
	batches map[uint64]batchInfo
	buf     []byte
	getBuf  []byte

	first int
	size  int
}

type batchInfo struct {
	index     int
	length    uint32
	collected uint32
}

type batchHeader struct {
	batchID uint64
}

const batchHeaderSize = int(unsafe.Sizeof(batchHeader{}))

// InitStore ...
func InitStore(s *Store, bufSize int, maxBatchSize int) {
	s.batches = map[uint64]batchInfo{}
	s.buf = make([]byte, bufSize)
	s.getBuf = make([]byte, maxBatchSize+batchHeaderSize)
	s.first = 0
	s.size = 0
}

func (s *Store) readAt(data []byte, index int) {
	index = index % len(s.buf)
	copy(data, s.buf[index:])
	if index+len(data) > len(s.buf) {
		firstPart := len(s.buf) - index
		secondPart := len(data) - firstPart
		copy(data[firstPart:], s.buf[:secondPart])
	}
}

func (s *Store) writeAt(index int, data []byte) {
	index = index % len(s.buf)
	copy(s.buf[index:], data)
	if index+len(data) > len(s.buf) {
		firstPart := len(s.buf) - index
		secondPart := len(data) - firstPart
		copy(s.buf[:secondPart], data[firstPart:])
	}
}

func (s *Store) unusedSize() int {
	return len(s.buf) - s.size
}

func (s *Store) reclaim(n int) {
	s.first = (s.first + n) % len(s.buf)
	s.size -= n
}

func (s *Store) deleteLeastRecent(length uint32) {
	size := batchHeaderSize + int(length)
	var batchHeaderData [batchHeaderSize]byte

	for s.unusedSize() < size {
		s.readAt(batchHeaderData[:], s.first)
		header := (*batchHeader)(unsafe.Pointer(&batchHeaderData[0]))
		info := s.batches[header.batchID]
		delete(s.batches, header.batchID)
		s.reclaim(batchHeaderSize + int(info.length))
	}
}

// Put ...
func (s *Store) Put(
	batchID uint64, length uint32, offset uint32, data []byte,
) bool {
	if offset+uint32(len(data)) > length {
		delete(s.batches, batchID)
		return false
	}

	info, ok := s.batches[batchID]
	if !ok {
		s.deleteLeastRecent(length)

		info = batchInfo{
			index:     s.first,
			length:    length,
			collected: 0,
		}

		var batchHeaderData [batchHeaderSize]byte
		header := (*batchHeader)(unsafe.Pointer(&batchHeaderData[0]))
		header.batchID = batchID
		s.writeAt(info.index, batchHeaderData[:])

		s.size += batchHeaderSize + int(length)
	}

	s.writeAt(info.index+batchHeaderSize+int(offset), data)

	info.collected += uint32(len(data))
	s.batches[batchID] = info

	return info.collected == info.length
}

// Get ...
func (s *Store) Get(batchID uint64) []byte {
	info, ok := s.batches[batchID]
	if !ok {
		return nil
	}
	s.readAt(s.getBuf[:info.length], info.index+batchHeaderSize)
	return s.getBuf[:info.length]
}
