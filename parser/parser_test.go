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

func TestParser_LGET_Missing_CRLF(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	err := p.Process([]byte("LGET somekey"))

	assert.Equal(t, errors.New("missing CRLF"), err)
}

func TestParser_LGET_Missing_CRLF_With_Number(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	err := p.Process([]byte("LGET somekey 123"))

	assert.Equal(t, errors.New("missing CRLF"), err)
}

func TestParser_LGET_Key_As_Number(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	handler.OnLGETFunc = func(key []byte) {}
	err := p.Process([]byte("LGET 123\r\n"))

	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(handler.OnLGETCalls()))
	assert.Equal(t, []byte("123"), handler.OnLGETCalls()[0].Key)
}

func TestParser_LSET_Missing_Key(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	err := p.Process([]byte("LSET"))

	assert.Equal(t, errors.New("missing key"), err)
}

func TestParser_LSET_Missing_Lease(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	err := p.Process([]byte("LSET somekey"))

	assert.Equal(t, errors.New("missing lease"), err)
}

func TestParser_LSET_Lease_Not_Number(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	err := p.Process([]byte("LSET somekey lease"))

	assert.Equal(t, errors.New("lease is not number"), err)
}

func TestParser_LSET_Missing_Size(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	err := p.Process([]byte("LSET key01 1234"))

	assert.Equal(t, errors.New("missing size"), err)
}

func TestParser_LSET_Size_Not_Number(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	err := p.Process([]byte("LSET key01 1234 some-size"))

	assert.Equal(t, errors.New("size is not number"), err)
}

func TestParser_LSET_Missing_CRLF(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	err := p.Process([]byte("LSET key01 1234 20"))

	assert.Equal(t, errors.New("missing CRLF"), err)
}

func TestParser_LSET_Not_CRLF(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	err := p.Process([]byte("LSET key01 1234 20 another"))

	assert.Equal(t, errors.New("missing CRLF"), err)
}

func TestParser_LSET_Missing_Data(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	err := p.Process([]byte("LSET key01 1234 8 \r\nabcd"))

	assert.Equal(t, errors.New("missing data"), err)
}

func TestParser_LSET_Missing_Data_CRLF(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	err := p.Process([]byte("LSET key01 1234 8 \r\nabcd1234"))

	assert.Equal(t, errors.New("missing CRLF"), err)
}

func TestParser_LSET_Wrong_Data_CRLF(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	err := p.Process([]byte("LSET key01 1234 8 \r\nabcd123456"))

	assert.Equal(t, errors.New("missing CRLF"), err)
}

// TODO Validate DEL
