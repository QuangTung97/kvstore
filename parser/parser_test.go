package parser

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func newParser(handler CommandHandler) *Parser {
	p := &Parser{}
	InitParser(p, handler)
	return p
}

func TestParser(t *testing.T) {
	handler := &CommandHandlerMock{}
	p := newParser(handler)

	handler.OnLGETFunc = func(key []byte) {}
	p.Process([]byte("LGET some-key\r\n"))

	assert.Equal(t, 1, len(handler.OnLGETCalls()))
	assert.Equal(t, []byte("some-key"), handler.OnLGETCalls()[0].Key)
}
