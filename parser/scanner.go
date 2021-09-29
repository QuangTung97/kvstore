package parser

import (
	"bytes"
)

type tokenType int

const (
	tokenTypeLGET tokenType = iota
	tokenTypeLSET
	tokenTypeDEL
	tokenTypeIdent

	tokenTypeInt
	tokenTypeCRLF
	tokenTypeBinary
)

type scannerState int

const (
	scannerStateInit scannerState = iota
	scannerStateIdent
	scannerStateCR
	scannerStateCRLF
	scannerStateNumber
)

type token struct {
	tokenType tokenType
	begin     int
	end       int
}

func (t token) getData(data []byte) []byte {
	return data[t.begin:t.end]
}

type scanner struct {
	tokens []token
	state  scannerState
	begin  int
}

func initScanner(s *scanner) {
	s.tokens = make([]token, 0, 8)
}

const (
	charSpace = ' '
	charLF    = '\n'
	charCR    = '\r'
	charTab   = '\t'
	charNull  = '\x00'
)

func (s *scanner) scan(data []byte) {
	s.state = scannerStateInit
	s.begin = 0
	for index, c := range data {
		s.handleChar(data, index, c)
	}
	s.handleChar(data, len(data), charSpace)
}

func isWhitespaceChar(c byte) bool {
	return c == charSpace || c == charTab || c == charLF || c == charNull
}

func isDigitChar(c byte) bool {
	return c >= '0' && c <= '9'
}

func (s *scanner) handleCharForInitState(index int, c byte) {
	if isWhitespaceChar(c) {
		return
	}

	s.begin = index
	if c == charCR {
		s.state = scannerStateCR
		return
	}
	if isDigitChar(c) {
		s.state = scannerStateNumber
		return
	}
	s.state = scannerStateIdent
}

func (s *scanner) gotoInitState(index int, c byte) {
	s.state = scannerStateInit
	s.handleCharForInitState(index, c)
}

func (s *scanner) handleChar(data []byte, index int, c byte) {
	switch s.state {
	case scannerStateInit:
		s.handleCharForInitState(index, c)

	case scannerStateIdent:
		if !isWhitespaceChar(c) && c != charCR {
			return
		}
		s.appendIdent(data, index)

	case scannerStateCR:
		if c == charLF {
			s.state = scannerStateCRLF
			return
		}

	case scannerStateCRLF:
		s.appendCRLF()

	case scannerStateNumber:
		if isDigitChar(c) {
			return
		}
		s.appendInt(index)
	}
	s.gotoInitState(index, c)
}

func computeTokenType(data []byte) tokenType {
	switch data[0] {
	case 'L':
		if bytes.Equal(data, LGET) {
			return tokenTypeLGET
		}
		if bytes.Equal(data, LSET) {
			return tokenTypeLSET
		}
	case 'D':
		if bytes.Equal(data, DEL) {
			return tokenTypeDEL
		}
	}
	return tokenTypeIdent
}

func (s *scanner) appendIdent(data []byte, end int) {
	s.tokens = append(s.tokens, token{
		tokenType: computeTokenType(data[s.begin:end]),
		begin:     s.begin,
		end:       end,
	})
}

func (s *scanner) appendCRLF() {
	s.tokens = append(s.tokens, token{
		tokenType: tokenTypeCRLF,
		begin:     s.begin,
		end:       s.begin + 2,
	})
}

func (s *scanner) appendInt(end int) {
	s.tokens = append(s.tokens, token{
		tokenType: tokenTypeInt,
		begin:     s.begin,
		end:       end,
	})
}
