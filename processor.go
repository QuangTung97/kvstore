package kvstore

import (
	"fmt"
	"github.com/QuangTung97/kvstore/lease"
	"github.com/QuangTung97/kvstore/parser"
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

	resultData []byte
	sendFrame  []byte
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
		sendFrame:  make([]byte, options.maxResultPackageSize),
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

	data := cmdList.data
	for {
		requestID, content, offset := parseDataFrameEntry(data)
		if len(content) == 0 {
			p.options.logger.Error("parseDataFrameEntry error")
			return true
		}

		err := p.parser.Process(content)
		fmt.Println(err)
		fmt.Println(requestID)

		if offset >= len(data) {
			break
		}
	}

	p.cmdStore.commitProcessedOffset(committedOffset)
	return true
}

func (p *processor) OnLGET(key []byte) {
	fmt.Println("ON GET", key)
}

func (p *processor) OnLSET(key []byte, leaseID uint32, value []byte) {

}

func (p *processor) OnDEL(key []byte) {

}
