package parser

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func newParser(handler CommandHandler) *Parser {
	p := &Parser{}
	InitParser(p, handler)
	return p
}

func TestBytesToUint32(t *testing.T) {
	n := bytesToUint32([]byte("1234"))
	assert.Equal(t, uint32(1234), n)

	n = bytesToUint32([]byte("0"))
	assert.Equal(t, uint32(0), n)
}

func TestParser_LGET(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	handler.OnLGETFunc = func(key []byte) {}
	err := p.Process([]byte("LGET some-key\r\n"))

	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(handler.OnLGETCalls()))
	assert.Equal(t, []byte("some-key"), handler.OnLGETCalls()[0].Key)
}

func TestParser_LSET(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	handler.OnLSETFunc = func(key []byte, lease uint32, value []byte) {}
	err := p.Process([]byte("LSET some-key 1234 10\r\nsome-value\r\n"))

	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(handler.OnLSETCalls()))
	assert.Equal(t, []byte("some-key"), handler.OnLSETCalls()[0].Key)
	assert.Equal(t, uint32(1234), handler.OnLSETCalls()[0].Lease)
	assert.Equal(t, []byte("some-value"), handler.OnLSETCalls()[0].Value)
}

func TestParser_DEL(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	handler.OnDELFunc = func(key []byte) {}
	err := p.Process([]byte("DEL some-key\r\n"))

	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(handler.OnDELCalls()))
	assert.Equal(t, []byte("some-key"), handler.OnDELCalls()[0].Key)
}

func TestParser_Missing_Token(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	err := p.Process([]byte(""))
	assert.Equal(t, errors.New("missing command"), err)
}

func TestParser_Missing_Command(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	err := p.Process([]byte("\r\n"))
	assert.Equal(t, errors.New("missing command"), err)
}

func TestParser_Wrong_Command(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	err := p.Process([]byte("some-key\r\n"))
	assert.Equal(t, errors.New("invalid command"), err)
}

func TestParser_LGET_Only_Cmd(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	err := p.Process([]byte("LGET"))

	assert.Equal(t, errors.New("missing key"), err)
}

func TestParser_LGET_Missing_Key(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	err := p.Process([]byte("LGET \r\n  "))

	assert.Equal(t, errors.New("missing key"), err)
}

// TODO more validation
