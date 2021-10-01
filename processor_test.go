package kvstore

import (
	"github.com/QuangTung97/kvstore/lease"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func newProcessorForTest(sender ResponseSender, options ...Option) *processor {
	cache := lease.New(4, 1<<16)
	return newProcessor(1000, cache, sender, computeOptions(options...))
}

func newIPv4(a, b, c, d byte) net.IP {
	return net.IPv4(a, b, c, d).To4()
}

func (p *processor) perform(
	ip net.IP, port uint16, requestID uint64, action string,
) {
	data := make([]byte, 1000)
	offset := buildDataFrameEntry(data, requestID, []byte(action))
	p.appendCommands(ip, port, data[:offset])
}

func TestProcessor_RunSingleLoop(t *testing.T) {
	sender := &ResponseSenderMock{}
	p := newProcessorForTest(sender)

	p.perform(newIPv4(192, 168, 1, 23),
		7200, 200, "LGET key01\r\n")

	sender.SendFunc = func(ip net.IP, port uint16, data []byte) error { return nil }
	p.runSingleLoop()

	assert.Equal(t, 1, len(sender.SendCalls()))
}
