package kvstore

import (
	"github.com/QuangTung97/kvstore/lease"
	"github.com/QuangTung97/kvstore/parser"
	"go.uber.org/zap"
)

//go:generate moq -out processor_mocks_test.go . ResponseSender

// ResponseSender ...
type ResponseSender interface {
	Send(ip IPAddr, port uint16, data []byte) error
}

type processor struct {
	options kvstoreOptions

	cmdStore commandListStore
	parser   parser.Parser

	cache  *lease.Cache
	sender ResponseSender

	currentIP        IPAddr
	currentPort      uint16
	currentRequestID uint64

	resultData []byte

	sendData   []byte
	sendOffset int

	sendFrame      []byte
	currentBatchID uint64
}

func newProcessor(
	cache *lease.Cache,
	sender ResponseSender, options kvstoreOptions,
) *processor {
	p := &processor{
		options: options,

		cache:  cache,
		sender: sender,

		resultData:     make([]byte, options.bufferSize),
		sendData:       make([]byte, options.bufferSize),
		sendFrame:      make([]byte, options.maxResultPackageSize),
		currentBatchID: 0,
	}
	initCommandListStore(&p.cmdStore, options.bufferSize)
	parser.InitParser(&p.parser, p)
	return p
}

func (p *processor) isCommandAppendable(dataSize int) bool {
	return p.cmdStore.isCommandAppendable(dataSize)
}

func (p *processor) appendCommands(ip IPAddr, port uint16, data []byte) {
	p.cmdStore.appendCommands(ip, port, data)
}

func (p *processor) run() {
	for {
		continued := p.runSingleLoop()
		if !continued {
			return
		}
	}
}

func (p *processor) shutdown() {
	p.cmdStore.stopWait()
}

func (p *processor) runSingleLoop() bool {
	continued := p.cmdStore.waitAvailable()
	if !continued {
		return false
	}

	cmdList, committedOffset := p.cmdStore.getNextRawCommandList()
	defer p.cmdStore.commitProcessedOffset(committedOffset)

	p.currentIP = cmdList.ip
	p.currentPort = cmdList.port
	p.sendOffset = 0

	data := cmdList.data
	for len(data) > 0 {
		requestID, content, nextOffset := parseDataFrameEntry(data)
		if len(content) == 0 {
			p.options.logger.Error("Invalid data frame entry")
			return true
		}

		p.currentRequestID = requestID

		err := p.parser.Process(content)
		if err != nil {
			p.onCommand(func(data []byte) int {
				return buildErrorResponse(data, err.Error())
			})
		}
		data = data[nextOffset:]
	}

	p.sendResponse()
	return true
}

func (p *processor) sendResultFrame(data []byte) {
	err := p.sender.Send(p.currentIP, p.currentPort, data)
	if err != nil {
		p.options.logger.Error("Send response error", zap.Error(err))
		return
	}
}

func (p *processor) sendResponse() {
	p.currentBatchID++

	length := p.sendOffset
	data := p.sendData[:length]
	offset := uint32(0)
	sendFrameLen := len(p.sendFrame)

	if length+dataFrameLengthOffset <= sendFrameLen {
		nextOffset := buildDataFrameHeader(p.sendFrame, dataFrameHeader{
			batchID:    p.currentBatchID,
			fragmented: false,
		})

		copy(p.sendFrame[nextOffset:], data)
		nextOffset += length
		p.sendResultFrame(p.sendFrame[:nextOffset])
		return
	}

	for len(data) > 0 {
		nextOffset := buildDataFrameHeader(p.sendFrame, dataFrameHeader{
			batchID:    p.currentBatchID,
			fragmented: true,
			length:     uint32(length),
			offset:     offset,
		})

		dataLen := len(data)
		if nextOffset+len(data) > sendFrameLen {
			dataLen = sendFrameLen - nextOffset
		}

		copy(p.sendFrame[nextOffset:], data)
		nextOffset += dataLen

		p.sendResultFrame(p.sendFrame[:nextOffset])

		data = data[dataLen:]
		offset += uint32(dataLen)
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
var rejectedResponse = []byte("REJECTED")
var crlfResponse = []byte("\r\n")
var errorResponse = []byte("ERROR ")

func buildGetResponse(data []byte, result lease.GetResult, value []byte) int {
	offset := 0

	switch result.Status {
	case lease.GetStatusLeaseGranted:
		copy(data, grantedResponse)
		offset = len(grantedResponse)

		offset += buildResponseNumber(data[offset:], uint64(result.LeaseID))

	case lease.GetStatusLeaseRejected:
		copy(data, rejectedResponse)
		offset = len(rejectedResponse)

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

//revive:disable-next-line:flag-parameter
func buildOKResponse(data []byte, affected bool) int {
	num := uint64(0)
	if affected {
		num = 1
	}
	copy(data, okResponse)
	offset := len(okResponse)

	offset += buildResponseNumber(data[offset:], num)

	copy(data[offset:], crlfResponse)
	offset += len(crlfResponse)

	return offset
}

func buildErrorResponse(data []byte, errMsg string) int {
	copy(data, errorResponse)
	offset := len(errorResponse)

	copy(data[offset:], errMsg)
	offset += len(errMsg)

	copy(data[offset:], crlfResponse)
	offset += len(crlfResponse)

	return offset
}

func (p *processor) onCommand(builder func(data []byte) int) {
	offset := p.sendOffset + entryDataOffset

	dataSize := builder(p.sendData[offset:])
	offset += dataSize

	buildDataFrameEntryHeader(p.sendData[p.sendOffset:], p.currentRequestID, dataSize)

	p.sendOffset = offset
}

func (p *processor) OnLGET(key []byte) {
	result := p.cache.Get(key, p.resultData)

	p.onCommand(func(data []byte) int {
		return buildGetResponse(data, result, p.resultData[:result.ValueSize])
	})
}

func (p *processor) OnLSET(key []byte, leaseID uint32, value []byte) {
	affected := p.cache.Set(key, leaseID, value)

	p.onCommand(func(data []byte) int {
		return buildOKResponse(data, affected)
	})
}

func (p *processor) OnDEL(key []byte) {
	affected := p.cache.Invalidate(key)

	p.onCommand(func(data []byte) int {
		return buildOKResponse(data, affected)
	})
}
