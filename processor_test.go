package kvstore

import (
	"github.com/QuangTung97/kvstore/kvstorepb"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func TestProcessor(t *testing.T) {
	p := newProcessor(1024, nil)
	assert.Equal(t, uint64(0), p.nextOffset)
	p.appendCommands(net.IPv4(192, 168, 0, 1), 8100, []byte("some-data"))
	assert.Equal(t, uint64(20+len("some-data")), p.nextOffset)

	assert.Equal(t, rawCommandList{
		ip:            net.IPv4(192, 168, 0, 1),
		port:          8100,
		data:          []byte("some-data"),
		nextProcessed: 20 + uint64(len("some-data")),
	}, p.getNextRawCommandList())
}

// TODO append commands wrap around

func TestParseRawCommandList(t *testing.T) {
	cmdList := &kvstorepb.CommandList{
		Commands: []*kvstorepb.Command{
			{
				Type: kvstorepb.CommandType_COMMAND_TYPE_LEASE_GET,
				Id:   100,
				LeaseGet: &kvstorepb.CommandLeaseGet{
					Key: "some-key",
				},
			},
		},
	}

	data, err := cmdList.Marshal()
	if err != nil {
		panic(err)
	}

	result, err := parseRawCommandList(rawCommandList{
		ip:   net.IPv4(192, 168, 0, 1),
		port: 8100,
		data: data,
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, commandList{
		ip:   net.IPv4(192, 168, 0, 1),
		port: 8100,
		commands: []*kvstorepb.Command{
			{
				Type: kvstorepb.CommandType_COMMAND_TYPE_LEASE_GET,
				Id:   100,
				LeaseGet: &kvstorepb.CommandLeaseGet{
					Key: "some-key",
				},
			},
		},
	}, result)
}
