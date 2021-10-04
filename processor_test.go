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

func TestBuildGetResponse_Found(t *testing.T) {
	data := make([]byte, 1000)
	offset := buildGetResponse(data, lease.GetResult{
		Status:    lease.GetStatusFound,
		ValueSize: 1230,
	}, []byte("some value"))
	assert.Equal(t, []byte("OK 1230\r\nsome value\r\n"), data[:offset])
}

func TestBuildGetResponse_Granted_Lease_Zero(t *testing.T) {
	data := make([]byte, 1000)
	offset := buildGetResponse(data, lease.GetResult{
		Status:  lease.GetStatusLeaseGranted,
		LeaseID: 0,
	}, nil)
	assert.Equal(t, []byte("GRANTED 0\r\n"), data[:offset])
}

func TestBuildGetResponse_Granted_Normal(t *testing.T) {
	data := make([]byte, 1000)
	offset := buildGetResponse(data, lease.GetResult{
		Status:  lease.GetStatusLeaseGranted,
		LeaseID: 12340,
	}, nil)
	assert.Equal(t, []byte("GRANTED 12340\r\n"), data[:offset])
}

func (p *processor) perform(
	ip net.IP, port uint16, startRequestID uint64,
	actionList ...string,
) {
	data := make([]byte, 1000)

	offset := 0
	for _, action := range actionList {
		buildDataFrameEntryHeader(data[offset:], startRequestID, len(action))
		offset += entryDataOffset

		copy(data[offset:], action)
		offset += len(action)

		startRequestID++
	}
	p.appendCommands(ip, port, data[:offset])
}

func TestProcessor_RunSingleLoop_Single_Command(t *testing.T) {
	sender := &ResponseSenderMock{}
	p := newProcessorForTest(sender)

	p.perform(newIPv4(192, 168, 1, 23),
		7200, 213, "LGET key01\r\n")

	sender.SendFunc = func(ip net.IP, port uint16, data []byte) error { return nil }
	continued := p.runSingleLoop()
	assert.Equal(t, true, continued)

	assert.Equal(t, 1, len(sender.SendCalls()))
	assert.Equal(t, newIPv4(192, 168, 1, 23), sender.SendCalls()[0].IP)
	assert.Equal(t, uint16(7200), sender.SendCalls()[0].Port)

	requestID, data, _ := parseDataFrameEntry(sender.SendCalls()[0].Data)
	assert.Equal(t, uint64(213), requestID)
	assert.Equal(t, string(data), "GRANTED 1\r\n")
}

func TestProcessor_RunSingleLoop_Multi_Commands(t *testing.T) {
	sender := &ResponseSenderMock{}
	p := newProcessorForTest(sender)

	p.perform(newIPv4(192, 168, 1, 23),
		7200, 213,
		"LGET key01\r\n",
		"LGET key02\r\n",
	)

	sender.SendFunc = func(ip net.IP, port uint16, data []byte) error { return nil }
	p.runSingleLoop()

	assert.Equal(t, 1, len(sender.SendCalls()))
	assert.Equal(t, newIPv4(192, 168, 1, 23), sender.SendCalls()[0].IP)
	assert.Equal(t, uint16(7200), sender.SendCalls()[0].Port)

	sendData := sender.SendCalls()[0].Data

	requestID, data, offset := parseDataFrameEntry(sendData)
	assert.Equal(t, uint64(213), requestID)
	assert.Equal(t, string(data), "GRANTED 1\r\n")

	sendData = sendData[offset:]
	requestID, data, offset = parseDataFrameEntry(sendData)
	assert.Equal(t, uint64(214), requestID)
	assert.Equal(t, string(data), "GRANTED 1\r\n")
}

func TestProcessor_RunSingleLoop_LGET_Rejected(t *testing.T) {
	sender := &ResponseSenderMock{}
	p := newProcessorForTest(sender)

	p.perform(newIPv4(192, 168, 1, 23),
		7200, 213,
		"LGET key01\r\n",
		"LGET key01\r\n",
	)

	sender.SendFunc = func(ip net.IP, port uint16, data []byte) error { return nil }
	p.runSingleLoop()

	assert.Equal(t, 1, len(sender.SendCalls()))
	assert.Equal(t, newIPv4(192, 168, 1, 23), sender.SendCalls()[0].IP)
	assert.Equal(t, uint16(7200), sender.SendCalls()[0].Port)

	sendData := sender.SendCalls()[0].Data

	requestID, data, offset := parseDataFrameEntry(sendData)
	assert.Equal(t, uint64(213), requestID)
	assert.Equal(t, "GRANTED 1\r\n", string(data))

	sendData = sendData[offset:]
	requestID, data, offset = parseDataFrameEntry(sendData)
	assert.Equal(t, uint64(214), requestID)
	assert.Equal(t, "REJECTED\r\n", string(data))
}

func TestProcessor_RunSingleLoop_SET_OK(t *testing.T) {
	sender := &ResponseSenderMock{}
	p := newProcessorForTest(sender)

	sender.SendFunc = func(ip net.IP, port uint16, data []byte) error { return nil }

	p.perform(newIPv4(192, 168, 1, 23),
		7200, 213,
		"LGET key01\r\n",
	)

	p.runSingleLoop()

	assert.Equal(t, 1, len(sender.SendCalls()))

	sendData := sender.SendCalls()[0].Data

	requestID, data, _ := parseDataFrameEntry(sendData)
	assert.Equal(t, uint64(213), requestID)
	assert.Equal(t, string(data), "GRANTED 1\r\n")

	p.perform(newIPv4(192, 168, 1, 23),
		7200, 220,
		"LSET key01 1 10\r\nsome-value\r\n",
	)

	p.runSingleLoop()
	assert.Equal(t, 2, len(sender.SendCalls()))

	sendData = sender.SendCalls()[1].Data
	requestID, data, _ = parseDataFrameEntry(sendData)
	assert.Equal(t, uint64(220), requestID)
	assert.Equal(t, "OK 1\r\n", string(data))
}

func TestProcessor_RunSingleLoop_SET_Not_Affected(t *testing.T) {
	sender := &ResponseSenderMock{}
	p := newProcessorForTest(sender)

	sender.SendFunc = func(ip net.IP, port uint16, data []byte) error { return nil }

	p.perform(newIPv4(192, 168, 1, 23),
		7200, 213,
		"LGET key01\r\n",
	)

	p.runSingleLoop()

	assert.Equal(t, 1, len(sender.SendCalls()))

	sendData := sender.SendCalls()[0].Data

	requestID, data, _ := parseDataFrameEntry(sendData)
	assert.Equal(t, uint64(213), requestID)
	assert.Equal(t, string(data), "GRANTED 1\r\n")

	p.perform(newIPv4(192, 168, 1, 23),
		7200, 220,
		"LSET key01 2 10\r\nsome-value\r\n",
	)

	p.runSingleLoop()
	assert.Equal(t, 2, len(sender.SendCalls()))

	sendData = sender.SendCalls()[1].Data
	requestID, data, _ = parseDataFrameEntry(sendData)
	assert.Equal(t, uint64(220), requestID)
	assert.Equal(t, "OK 0\r\n", string(data))
}

func TestProcessor_RunSingleLoop_DEL_OK(t *testing.T) {
	sender := &ResponseSenderMock{}
	p := newProcessorForTest(sender)

	sender.SendFunc = func(ip net.IP, port uint16, data []byte) error { return nil }

	// LGET
	p.perform(newIPv4(192, 168, 1, 23),
		7200, 213,
		"LGET key01\r\n",
	)

	p.runSingleLoop()

	sendData := sender.SendCalls()[0].Data
	requestID, data, _ := parseDataFrameEntry(sendData)
	assert.Equal(t, uint64(213), requestID)
	assert.Equal(t, string(data), "GRANTED 1\r\n")

	// LSET
	p.perform(newIPv4(192, 168, 1, 23),
		7200, 220,
		"LSET key01 1 10\r\nsome-value\r\n",
	)

	p.runSingleLoop()

	sendData = sender.SendCalls()[1].Data
	requestID, data, _ = parseDataFrameEntry(sendData)
	assert.Equal(t, uint64(220), requestID)
	assert.Equal(t, "OK 1\r\n", string(data))

	// DEL
	p.perform(newIPv4(192, 168, 1, 23),
		7200, 230,
		"DEL key01\r\n",
	)

	p.runSingleLoop()

	sendData = sender.SendCalls()[2].Data
	requestID, data, _ = parseDataFrameEntry(sendData)
	assert.Equal(t, uint64(230), requestID)
	assert.Equal(t, "OK 1\r\n", string(data))

	assert.Equal(t, 3, len(sender.SendCalls()))
}
