package kvstore

import (
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"net"
	"testing"
)

func newLogger() *zap.Logger {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return logger
}

func TestProcessor(t *testing.T) {
	p := newProcessor(1024, nil, nil, computeOptions())
	assert.Equal(t, uint64(0), p.nextOffset)
	p.appendCommands(net.IPv4(192, 168, 0, 1), 8100, []byte("some-data"))
	assert.Equal(t, uint64(20+len("some-data")), p.nextOffset)

	cmdList, nextProcessed := p.getNextRawCommandList()
	assert.Equal(t, rawCommandList{
		ip:   net.IPv4(192, 168, 0, 1),
		port: 8100,
		data: []byte("some-data"),
	}, cmdList)
	assert.Equal(t, 20+uint64(len("some-data")), nextProcessed)
}
