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

func TestScanner_Empty(t *testing.T) {
	s := newScanner()

	input := []byte("")
	s.scan(input)
	assert.Equal(t, []token{}, s.tokens)
}

func TestScanner_Simple_LGET(t *testing.T) {
	s := newScanner()

	input := []byte("LGET")
	s.scan(input)

	assert.Equal(t, []token{
		{
			tokenType: tokenTypeLGET,
			begin:     0,
			end:       len(LGET),
		},
	}, s.tokens)
	assert.Equal(t, []byte("LGET"), s.tokens[0].getData(input))
}

func TestScanner_LGET_With_Leading_Spaces(t *testing.T) {
	s := newScanner()

	input := []byte(" \t\nLGET")
	s.scan(input)

	assert.Equal(t, []token{
		{
			tokenType: tokenTypeLGET,
			begin:     3,
			end:       3 + len(LGET),
		},
	}, s.tokens)
	assert.Equal(t, []byte("LGET"), s.tokens[0].getData(input))
}

func TestScanner_LGET_With_Tailing_Spaces(t *testing.T) {
	s := newScanner()

	input := []byte(" \t\nLGET\x00 \t")
	s.scan(input)
	assert.Equal(t, []token{
		{
			tokenType: tokenTypeLGET,
			begin:     3,
			end:       3 + len(LGET),
		},
	}, s.tokens)
	assert.Equal(t, []byte("LGET"), s.tokens[0].getData(input))
}

func TestScanner_LSET(t *testing.T) {
	s := newScanner()

	input := []byte("LSET")
	s.scan(input)
	assert.Equal(t, []token{
		{
			tokenType: tokenTypeLSET,
			begin:     0,
			end:       len(LSET),
		},
	}, s.tokens)
	assert.Equal(t, []byte("LSET"), s.tokens[0].getData(input))
}

func TestScanner_DEL(t *testing.T) {
	s := newScanner()

	input := []byte(" DEL")
	s.scan(input)

	assert.Equal(t, []token{
		{
			tokenType: tokenTypeDEL,
			begin:     1,
			end:       1 + len(DEL),
		},
	}, s.tokens)
	assert.Equal(t, []byte("DEL"), s.tokens[0].getData(input))
}

func TestScanner_CRLF(t *testing.T) {
	s := newScanner()
	s.scan([]byte("\r\n"))
	assert.Equal(t, []token{
		{
			tokenType: tokenTypeCRLF,
			begin:     0,
			end:       2,
		},
	}, s.tokens)
}

func TestScanner_CRLF_With_Spaces(t *testing.T) {
	s := newScanner()
	s.scan([]byte(" \r\n "))
	assert.Equal(t, []token{
		{
			tokenType: tokenTypeCRLF,
			begin:     1,
			end:       3,
		},
	}, s.tokens)
}

func TestScanner_Indent(t *testing.T) {
	s := newScanner()
	s.scan([]byte(" some-key "))
	assert.Equal(t, []token{
		{
			tokenType: tokenTypeIdent,
			begin:     1,
			end:       9,
		},
	}, s.tokens)
}

func TestScanner_Only_CR(t *testing.T) {
	s := newScanner()
	s.scan([]byte("\r"))
	assert.Equal(t, []token{}, s.tokens)
}

func TestScanner_Int(t *testing.T) {
	s := newScanner()

	input := []byte("1234567890")
	s.scan(input)

	assert.Equal(t, 1, len(s.tokens))

	assert.Equal(t, tokenTypeInt, s.tokens[0].tokenType)
	assert.Equal(t, []byte("1234567890"), s.tokens[0].getData(input))
}

func TestScanner_Multiple_Tokens(t *testing.T) {
	s := newScanner()

	input := []byte("LGET some-key\r\n")
	s.scan(input)

	assert.Equal(t, 3, len(s.tokens))

	assert.Equal(t, tokenTypeLGET, s.tokens[0].tokenType)
	assert.Equal(t, tokenTypeIdent, s.tokens[1].tokenType)
	assert.Equal(t, tokenTypeCRLF, s.tokens[2].tokenType)

	assert.Equal(t, []byte("LGET"), s.tokens[0].getData(input))
	assert.Equal(t, []byte("some-key"), s.tokens[1].getData(input))
	assert.Equal(t, []byte("\r\n"), s.tokens[2].getData(input))
}

func TestScanner_Multiple_Tokens_With_Int(t *testing.T) {
	s := newScanner()

	input := []byte("LSET\r\n some-key 1235\r\n")
	s.scan(input)

	assert.Equal(t, 5, len(s.tokens))

	assert.Equal(t, tokenTypeLSET, s.tokens[0].tokenType)
	assert.Equal(t, tokenTypeCRLF, s.tokens[1].tokenType)
	assert.Equal(t, tokenTypeIdent, s.tokens[2].tokenType)
	assert.Equal(t, tokenTypeInt, s.tokens[3].tokenType)
	assert.Equal(t, tokenTypeCRLF, s.tokens[4].tokenType)

	assert.Equal(t, []byte("LSET"), s.tokens[0].getData(input))
	assert.Equal(t, []byte("\r\n"), s.tokens[1].getData(input))
	assert.Equal(t, []byte("some-key"), s.tokens[2].getData(input))
	assert.Equal(t, []byte("1235"), s.tokens[3].getData(input))
	assert.Equal(t, []byte("\r\n"), s.tokens[4].getData(input))
}

func TestScanner_Number_Concatenated_With_String(t *testing.T) {
	s := newScanner()

	input := []byte("1234some-name")
	s.scan(input)

	assert.Equal(t, 1, len(s.tokens))
	assert.Equal(t, tokenTypeIdent, s.tokens[0].tokenType)
	assert.Equal(t, []byte("1234some-name"), s.tokens[0].getData(input))
}

func TestScanner_ScanBinary(t *testing.T) {
	s := newScanner()

	input := []byte("some-value LGET \r\n")
	s.scanBinary(10, input)

	assert.Equal(t, 3, len(s.tokens))

	assert.Equal(t, tokenTypeBinary, s.tokens[0].tokenType)
	assert.Equal(t, tokenTypeLGET, s.tokens[1].tokenType)
	assert.Equal(t, tokenTypeCRLF, s.tokens[2].tokenType)

	assert.Equal(t, []byte("some-value"), s.tokens[0].getData(input))
	assert.Equal(t, []byte("LGET"), s.tokens[1].getData(input))
	assert.Equal(t, []byte("\r\n"), s.tokens[2].getData(input))
}

func TestScanner_ScanBinary_Not_Enough_Data(t *testing.T) {
	s := newScanner()

	input := []byte("random")
	s.scanBinary(10, input)

	assert.Equal(t, 0, len(s.tokens))
}
