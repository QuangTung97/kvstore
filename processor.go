package kvstore

import (
	"encoding/binary"
	"errors"
	"github.com/QuangTung97/kvstore/kvstorepb"
	"github.com/QuangTung97/memtable"
	"net"
	"sync"
	"sync/atomic"
)

type processor struct {
	mut  sync.Mutex
	cond *sync.Cond

	cache *memtable.Memtable

	buffer     []byte
	nextOffset uint64
	processed  uint64
	resultList []*kvstorepb.CommandResult
}

func newProcessor(buffSize int, cache *memtable.Memtable) *processor {
	p := &processor{
		buffer: make([]byte, buffSize),
		cache:  cache,
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

type commandListHeader struct {
	batchID uint32
	length  uint32
	offset  uint32
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
		p.processCommandList(cmdList)
		p.updateProcessed(nextProcessed)
	}
}

func memtableStatusToLeaseGetStatus(status memtable.GetStatus) kvstorepb.LeaseGetStatus {
	switch status {
	case memtable.GetStatusFound:
		return kvstorepb.LeaseGetStatus_LEASE_GET_STATUS_FOUND
	case memtable.GetStatusLeaseGranted:
		return kvstorepb.LeaseGetStatus_LEASE_GET_STATUS_LEASE_GRANTED
	case memtable.GetStatusLeaseRejected:
		return kvstorepb.LeaseGetStatus_LEASE_GET_STATUS_LEASE_REJECTED
	default:
		panic("invalid memtable status")
	}
}

func (p *processor) processLeaseGet(id uint64, cmd *kvstorepb.CommandLeaseGet) {
	if cmd == nil {
		// TODO Logging
		return
	}
	result := p.cache.Get([]byte(cmd.Key))
	p.resultList = append(p.resultList, &kvstorepb.CommandResult{
		Type: kvstorepb.CommandType_COMMAND_TYPE_LEASE_GET,
		Id:   id,
		LeaseGet: &kvstorepb.CommandLeaseGetResult{
			Status:  memtableStatusToLeaseGetStatus(result.Status),
			LeaseId: uint64(result.LeaseID),
			Value:   string(result.Value),
		},
	})
}

func (p *processor) processLeaseSet(id uint64, cmd *kvstorepb.CommandLeaseSet) {
	if cmd == nil {
		// TODO Logging
		return
	}
	affected := p.cache.Set([]byte(cmd.Key), uint32(cmd.LeaseId), []byte(cmd.Value))
	p.resultList = append(p.resultList, &kvstorepb.CommandResult{
		Type: kvstorepb.CommandType_COMMAND_TYPE_LEASE_SET,
		Id:   id,
		LeaseSet: &kvstorepb.CommandLeaseSetResult{
			Affected: affected,
		},
	})
}

func (p *processor) processCommandList(rawCmdList rawCommandList) {
	cmdList, err := parseRawCommandList(rawCmdList.data)
	if err != nil {
		// TODO logging
		return
	}

	for _, cmd := range cmdList {
		switch cmd.Type {
		case kvstorepb.CommandType_COMMAND_TYPE_LEASE_GET:
			p.processLeaseGet(cmd.Id, cmd.LeaseGet)

		case kvstorepb.CommandType_COMMAND_TYPE_LEASE_SET:

		case kvstorepb.CommandType_COMMAND_TYPE_INVALIDATE:

		default:
			// TODO logging
		}
	}
	// TODO Finishing
}

func (p *processor) updateProcessed(value uint64) {
	atomic.StoreUint64(&p.processed, value)
}

func (p *processor) loadProcessed() uint64 {
	return atomic.LoadUint64(&p.processed)
}

const dataLengthIDOffset = 4
const dataOffsetValueOffset = 8
const dataCmdListOffset = 12

func parseRawCommandListHeader(data []byte) commandListHeader {
	batchID := binary.LittleEndian.Uint32(data)
	length := binary.LittleEndian.Uint32(data[dataLengthIDOffset:])
	offset := binary.LittleEndian.Uint32(data[dataOffsetValueOffset:])

	return commandListHeader{
		batchID: batchID,
		length:  length,
		offset:  offset,
	}
}

func parseRawCommandList(data []byte) ([]*kvstorepb.Command, error) {
	var result []*kvstorepb.Command
	for len(data) > 0 {
		size, offset := binary.Uvarint(data)
		if offset <= 0 {
			return nil, errors.New("invalid command size")
		}

		if len(data) < offset + int(size) {
			return nil, errors.New("invalid command data size")
		}

		cmd := &kvstorepb.Command{}
		err := cmd.Unmarshal(data[offset : offset+int(size)])
		if err != nil {
			return nil, err
		}

		result = append(result, cmd)
		data = data[uint64(offset)+size:]
	}
	return result, nil
}
