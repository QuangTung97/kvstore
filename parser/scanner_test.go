package parser

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func newScanner() *scanner {
	s := &scanner{}
	initScanner(s)
	return s
}

func TestScanner_Simple_LGET(t *testing.T) {
	p := newScanner()

	input := []byte("LGET")
	p.scan(input)

	assert.Equal(t, []token{
		{
			tokenType: tokenTypeLGET,
			begin:     0,
			end:       len(LGET),
		},
	}, p.tokens)
	assert.Equal(t, []byte("LGET"), p.tokens[0].getData(input))
}

func TestScanner_LGET_With_Leading_Spaces(t *testing.T) {
	p := newScanner()

	input := []byte(" \t\nLGET")
	p.scan(input)

	assert.Equal(t, []token{
		{
			tokenType: tokenTypeLGET,
			begin:     3,
			end:       3 + len(LGET),
		},
	}, p.tokens)
	assert.Equal(t, []byte("LGET"), p.tokens[0].getData(input))
}

func TestScanner_LGET_With_Tailing_Spaces(t *testing.T) {
	p := newScanner()

	input := []byte(" \t\nLGET\x00 \t")
	p.scan(input)
	assert.Equal(t, []token{
		{
			tokenType: tokenTypeLGET,
			begin:     3,
			end:       3 + len(LGET),
		},
	}, p.tokens)
	assert.Equal(t, []byte("LGET"), p.tokens[0].getData(input))
}

func TestScanner_LSET(t *testing.T) {
	p := newScanner()

	input := []byte("LSET")
	p.scan(input)
	assert.Equal(t, []token{
		{
			tokenType: tokenTypeLSET,
			begin:     0,
			end:       len(LSET),
		},
	}, p.tokens)
	assert.Equal(t, []byte("LSET"), p.tokens[0].getData(input))
}

func TestScanner_DEL(t *testing.T) {
	p := newScanner()

	input := []byte(" DEL")
	p.scan(input)

	assert.Equal(t, []token{
		{
			tokenType: tokenTypeDEL,
			begin:     1,
			end:       1 + len(DEL),
		},
	}, p.tokens)
	assert.Equal(t, []byte("DEL"), p.tokens[0].getData(input))
}

func TestScanner_CRLF(t *testing.T) {
	p := newScanner()
	p.scan([]byte("\r\n"))
	assert.Equal(t, []token{
		{
			tokenType: tokenTypeCRLF,
			begin:     0,
			end:       2,
		},
	}, p.tokens)
}

func TestScanner_CRLF_With_Spaces(t *testing.T) {
	p := newScanner()
	p.scan([]byte(" \r\n "))
	assert.Equal(t, []token{
		{
			tokenType: tokenTypeCRLF,
			begin:     1,
			end:       3,
		},
	}, p.tokens)
}

func TestScanner_Indent(t *testing.T) {
	p := newScanner()
	p.scan([]byte(" some-key "))
	assert.Equal(t, []token{
		{
			tokenType: tokenTypeIdent,
			begin:     1,
			end:       9,
		},
	}, p.tokens)
}

func TestScanner_Only_CR(t *testing.T) {
	p := newScanner()
	p.scan([]byte("\r"))
	assert.Equal(t, []token{}, p.tokens)
}

func TestScanner_Int(t *testing.T) {
	p := newScanner()

	input := []byte("1234567890")
	p.scan(input)

	assert.Equal(t, 1, len(p.tokens))

	assert.Equal(t, tokenTypeInt, p.tokens[0].tokenType)
	assert.Equal(t, []byte("1234567890"), p.tokens[0].getData(input))
}

func TestScanner_Multiple_Tokens(t *testing.T) {
	p := newScanner()

	input := []byte("LGET some-key\r\n")
	p.scan(input)

	assert.Equal(t, 3, len(p.tokens))

	assert.Equal(t, tokenTypeLGET, p.tokens[0].tokenType)
	assert.Equal(t, tokenTypeIdent, p.tokens[1].tokenType)
	assert.Equal(t, tokenTypeCRLF, p.tokens[2].tokenType)

	assert.Equal(t, []byte("LGET"), p.tokens[0].getData(input))
	assert.Equal(t, []byte("some-key"), p.tokens[1].getData(input))
	assert.Equal(t, []byte("\r\n"), p.tokens[2].getData(input))
}

func TestScanner_Multiple_Tokens_With_Int(t *testing.T) {
	p := newScanner()

	input := []byte("LSET\r\n some-key 1235\r\n")
	p.scan(input)

	assert.Equal(t, 5, len(p.tokens))

	assert.Equal(t, tokenTypeLSET, p.tokens[0].tokenType)
	assert.Equal(t, tokenTypeCRLF, p.tokens[1].tokenType)
	assert.Equal(t, tokenTypeIdent, p.tokens[2].tokenType)
	assert.Equal(t, tokenTypeInt, p.tokens[3].tokenType)
	assert.Equal(t, tokenTypeCRLF, p.tokens[4].tokenType)

	assert.Equal(t, []byte("LSET"), p.tokens[0].getData(input))
	assert.Equal(t, []byte("\r\n"), p.tokens[1].getData(input))
	assert.Equal(t, []byte("some-key"), p.tokens[2].getData(input))
	assert.Equal(t, []byte("1235"), p.tokens[3].getData(input))
	assert.Equal(t, []byte("\r\n"), p.tokens[4].getData(input))
}
