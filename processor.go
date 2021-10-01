package kvstore

import (
	"github.com/QuangTung97/kvstore/lease"
	"github.com/QuangTung97/kvstore/parser"
	"go.uber.org/zap"
	"net"
)

//go:generate moq -out processor_mocks_test.go . ResponseSender

// ResponseSender ...
type ResponseSender interface {
	Send(ip net.IP, port uint16, data []byte) error
}

type processor struct {
	options kvstoreOptions

	cmdStore commandListStore
	parser   parser.Parser

	cache  *lease.Cache
	sender ResponseSender

	currentIP        net.IP
	currentPort      uint16
	currentRequestID uint64

	resultData []byte

	sendData   []byte
	sendOffset int
}

func newProcessor(
	buffSize int, cache *lease.Cache,
	sender ResponseSender, options kvstoreOptions,
) *processor {
	p := &processor{
		options: options,

		cache:  cache,
		sender: sender,

		resultData: make([]byte, buffSize),
		sendData:   make([]byte, buffSize),
	}
	initCommandListStore(&p.cmdStore, buffSize)
	parser.InitParser(&p.parser, p)
	return p
}

func (p *processor) isCommandAppendable(dataSize int) bool {
	return p.cmdStore.isCommandAppendable(dataSize)
}

func (p *processor) appendCommands(ip net.IP, port uint16, data []byte) {
	p.cmdStore.appendCommands(ip, port, data)
}

func (p *processor) runSingleLoop() bool {
	continued := p.cmdStore.waitAvailable()
	if !continued {
		return false
	}

	cmdList, committedOffset := p.cmdStore.getNextRawCommandList()

	p.currentIP = cmdList.ip
	p.currentPort = cmdList.port

	data := cmdList.data
	for {
		requestID, content, nextOffset := parseDataFrameEntry(data)
		if len(content) == 0 {
			return true
		}

		p.currentRequestID = requestID

		err := p.parser.Process(content)
		if err != nil {
			// TODO error return
		}

		if nextOffset >= len(data) {
			break
		}
		// TODO next data
	}

	p.sendResponse()

	p.cmdStore.commitProcessedOffset(committedOffset)
	return true
}

func (p *processor) sendResponse() {
	err := p.sender.Send(p.currentIP, p.currentPort, p.sendData[:p.sendOffset])
	if err != nil {
		p.options.logger.Error("Send response error", zap.Error(err))
		return
	}
}

func buildResponseNumber(data []byte, num uint64) int {
	if num == 0 {
		data[0] = '0'
		return 1
	}

	index := 0
	for num > 0 {
		c := byte(num % 10)
		data[index] = c + '0'
		index++
		num = num / 10
	}
	for i := 0; i < index/2; i++ {
		j := index - 1 - i
		data[j], data[i] = data[i], data[j]
	}
	return index
}

var okResponse = []byte("OK ")
var grantedResponse = []byte("GRANTED ")
var crlfResponse = []byte("\r\n")

func buildGetResponse(data []byte, result lease.GetResult, value []byte) int {
	offset := 0

	switch result.Status {
	case lease.GetStatusLeaseGranted:
		copy(data, grantedResponse)
		offset = len(grantedResponse)

		offset += buildResponseNumber(data[offset:], uint64(result.LeaseID))

	case lease.GetStatusLeaseRejected:

	default:
		copy(data, okResponse)
		offset = len(okResponse)

		offset += buildResponseNumber(data[offset:], uint64(result.ValueSize))

		copy(data[offset:], crlfResponse)
		offset += len(crlfResponse)

		copy(data[offset:], value)
		offset += len(value)
	}

	copy(data[offset:], crlfResponse)
	offset += len(crlfResponse)
	return offset
}

func (p *processor) OnLGET(key []byte) {
	result := p.cache.Get(key, p.resultData)

	offset := p.sendOffset + entryDataOffset

	dataSize := buildGetResponse(p.sendData[offset:],
		result, p.resultData[:result.ValueSize])
	offset += dataSize

	buildDataFrameEntryHeader(p.sendData[p.sendOffset:], p.currentRequestID, dataSize)

	p.sendOffset = offset
}

func (p *processor) OnLSET(key []byte, leaseID uint32, value []byte) {

}

func (p *processor) OnDEL(key []byte) {

}
