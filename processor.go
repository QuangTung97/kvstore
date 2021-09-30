package kvstore

import (
	"encoding/binary"
	"fmt"
	"github.com/QuangTung97/memtable"
	"net"
	"sync"
	"sync/atomic"
)

//go:generate moq -out processor_mocks_test.go . ResponseSender

// ResponseSender ...
type ResponseSender interface {
	Send(ip net.IP, port uint16, data []byte) error
}

type processor struct {
	mut  sync.Mutex
	cond *sync.Cond

	options kvstoreOptions
	cache   *memtable.Memtable
	sender  ResponseSender

	buffer     []byte
	nextOffset uint64
	processed  uint64

	resultData []byte
	sendFrame  []byte

	maxDataSendSize int
}

func newProcessor(
	buffSize int, cache *memtable.Memtable,
	sender ResponseSender, options kvstoreOptions,
) *processor {
	p := &processor{
		buffer: make([]byte, buffSize),

		options: options,
		cache:   cache,
		sender:  sender,

		resultData: make([]byte, buffSize),
		sendFrame:  make([]byte, options.maxResultPackageSize),
	}
	p.cond = sync.NewCond(&p.mut)
	return p
}

const portOffset = net.IPv6len
const lengthOffset = portOffset + 2
const dataOffset = lengthOffset + 2

type rawCommandList struct {
	ip   net.IP
	port uint16
	data []byte
}

func (p *processor) computeSlice(n uint16) []byte {
	begin := p.nextOffset
	end := begin + 16 + 2 + 2 + uint64(n)
	return p.buffer[begin:end]
}

func (p *processor) appendCommands(ip net.IP, port uint16, data []byte) {
	p.mut.Lock()

	length := uint16(len(data))

	slice := p.computeSlice(length)
	copy(slice, ip.To16())
	binary.LittleEndian.PutUint16(slice[portOffset:], port)
	binary.LittleEndian.PutUint16(slice[lengthOffset:], length)
	copy(slice[dataOffset:], data)
	p.nextOffset += uint64(len(slice))

	p.mut.Unlock()
	p.cond.Signal()
}

func (p *processor) getNextRawCommandList() (rawCommandList, uint64) {
	begin := p.processed

	ip := p.buffer[begin : begin+net.IPv6len]
	port := binary.LittleEndian.Uint16(p.buffer[begin+portOffset : begin+portOffset+2])
	length := binary.LittleEndian.Uint16(p.buffer[begin+lengthOffset : begin+lengthOffset+2])
	data := p.buffer[begin+dataOffset : begin+dataOffset+uint64(length)]

	return rawCommandList{
		ip:   ip,
		port: port,
		data: data,
	}, dataOffset + uint64(length)
}

func (p *processor) runSingleLoop() {
	p.mut.Lock()
	for p.processed >= p.nextOffset {
		p.cond.Wait()
	}
	nextOffset := p.nextOffset
	p.mut.Unlock()

	for p.processed < nextOffset {
		cmdList, nextProcessed := p.getNextRawCommandList()
		fmt.Println(cmdList)
		p.updateProcessed(nextProcessed)
	}
}

func (p *processor) updateProcessed(value uint64) {
	atomic.StoreUint64(&p.processed, value)
}

func (p *processor) loadProcessed() uint64 {
	return atomic.LoadUint64(&p.processed)
}
