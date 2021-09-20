package kvstore

import (
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func TestProcessor(t *testing.T) {
	p := newProcessor(1024)
	assert.Equal(t, uint64(0), p.nextOffset)
	p.appendCommands(net.IPv4(192, 168, 0, 1), 8100, []byte("some-data"))
	assert.Equal(t, uint64(20+len("some-data")), p.nextOffset)

	assert.Equal(t, command{
		ip:            net.IPv4(192, 168, 0, 1),
		port:          8100,
		data:          []byte("some-data"),
		nextProcessed: 20 + uint64(len("some-data")),
	}, p.getNextCommand())
}
