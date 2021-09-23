package kvstore

import (
	"encoding/binary"
	"github.com/QuangTung97/kvstore/kvstorepb"
	"github.com/QuangTung97/memtable"
	"go.uber.org/zap"
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

	resultList []*kvstorepb.CommandResult
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

		maxDataSendSize: options.maxResultPackageSize - dataFrameEntryListOffset,
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
		p.options.logger.Error("lease_get is empty")
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
		p.options.logger.Error("lease_set is empty")
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

type putResultStatus int

const (
	putResultStatusOK    putResultStatus = 1
	putResultStatusRetry putResultStatus = 2
	putResultStatusError putResultStatus = 3
)

func (p *processor) tryToPutDataOnResultData(
	responseCmd *kvstorepb.CommandResult, sizePlaceholder []byte,
	lastOffset int,
) (int, putResultStatus) {
	offset := lastOffset
	size := responseCmd.Size()

	sizeLen := binary.PutUvarint(sizePlaceholder, uint64(size))
	if offset+sizeLen > p.maxDataSendSize {
		return 0, putResultStatusRetry
	}
	copy(p.resultData[offset:], sizePlaceholder[:sizeLen])
	offset += sizeLen

	if offset+size > p.maxDataSendSize && lastOffset > 0 {
		return 0, putResultStatusRetry
	}

	_, err := responseCmd.MarshalToSizedBuffer(p.resultData[offset : offset+size])
	if err != nil {
		p.options.logger.Error("error when marshal protobuf", zap.Error(err))
		return 0, putResultStatusError
	}

	return offset + size, putResultStatusOK
}

func (p *processor) putDataOnResultData(
	ip net.IP, port uint16,
	responseCmd *kvstorepb.CommandResult, sizePlaceholder []byte,
	lastOffset int,
) (int, bool) {
	for {
		nextOffset, status := p.tryToPutDataOnResultData(responseCmd, sizePlaceholder, lastOffset)
		if status == putResultStatusError {
			return 0, false
		}
		if status == putResultStatusOK {
			return nextOffset, true
		}

		p.sendCommandResult(ip, port, lastOffset)
		lastOffset = 0
	}
}

func (p *processor) processCommandList(rawCmdList rawCommandList) {
	logger := p.options.logger

	//header := parseDataFrameHeader(rawCmdList.data)
	cmdList, err := parseRawCommandList(rawCmdList.data[dataFrameEntryListOffset:])
	if err != nil {
		logger.Error("error when parse raw command list", zap.Error(err))
		return
	}

	for _, cmd := range cmdList {
		switch cmd.Type {
		case kvstorepb.CommandType_COMMAND_TYPE_LEASE_GET:
			p.processLeaseGet(cmd.Id, cmd.LeaseGet)

		case kvstorepb.CommandType_COMMAND_TYPE_LEASE_SET:
			p.processLeaseSet(cmd.Id, cmd.LeaseSet)

		case kvstorepb.CommandType_COMMAND_TYPE_INVALIDATE:

		default:
			logger.Error("invalid command type", zap.Any("cmd.type", cmd.Type))
		}
	}

	var sizePlaceholder [binary.MaxVarintLen64]byte

	offset := 0
	for i, responseCmd := range p.resultList {
		p.resultList[i] = nil
		nextOffset, ok := p.putDataOnResultData(
			rawCmdList.ip, rawCmdList.port,
			responseCmd, sizePlaceholder[:],
			offset,
		)
		if !ok {
			return
		}
		offset = nextOffset
	}

	p.sendCommandResult(rawCmdList.ip, rawCmdList.port, offset)
}

func (p *processor) sendCommandResult(ip net.IP, port uint16, offset int) {
	for index := 0; index < offset; {
		nextIndex := index + p.maxDataSendSize
		if nextIndex > offset {
			nextIndex = offset
		}
		size := nextIndex - index

		buildDataFrameHeader(p.sendFrame, dataFrameHeader{
			batchID: 1, // TODO inc batch id
			length:  uint32(offset),
			offset:  uint32(index),
		})
		copy(p.sendFrame[dataFrameEntryListOffset:dataFrameEntryListOffset+size], p.resultData[index:nextIndex])

		sendData := p.sendFrame[:dataFrameEntryListOffset+size]
		err := p.sender.Send(ip, port, sendData)
		if err != nil {
			p.options.logger.Error("send command result error", zap.Error(err))
			return
		}

		index = nextIndex
	}
}

func (p *processor) updateProcessed(value uint64) {
	atomic.StoreUint64(&p.processed, value)
}

func (p *processor) loadProcessed() uint64 {
	return atomic.LoadUint64(&p.processed)
}
