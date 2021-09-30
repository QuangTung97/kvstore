package kvstore

import (
	"github.com/QuangTung97/memtable"
	"net"
)

//go:generate moq -out processor_mocks_test.go . ResponseSender

// ResponseSender ...
type ResponseSender interface {
	Send(ip net.IP, port uint16, data []byte) error
}

type processor struct {
	options kvstoreOptions

	cmdList commandListStore

	cache  *memtable.Memtable
	sender ResponseSender

	resultData []byte
	sendFrame  []byte
}

func newProcessor(
	buffSize int, cache *memtable.Memtable,
	sender ResponseSender, options kvstoreOptions,
) *processor {
	p := &processor{
		options: options,

		cache:  cache,
		sender: sender,

		resultData: make([]byte, buffSize),
		sendFrame:  make([]byte, options.maxResultPackageSize),
	}
	initCommandListStore(&p.cmdList, buffSize)
	return p
}

const portOffset = net.IPv4len
const lengthOffset = portOffset + 2
const dataOffset = lengthOffset + 2

type rawCommandList struct {
	ip   net.IP
	port uint16
	data []byte
}

func (*processor) runSingleLoop() {
}
