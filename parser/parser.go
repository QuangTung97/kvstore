package parser

import (
	"errors"
)

//go:generate moq -out parser_mocks_test.go . CommandHandler

// CommandHandler ...
type CommandHandler interface {
	OnLGET(key []byte)
	OnLSET(key []byte, lease uint32, value []byte)
	OnDEL(key []byte)
}

// ErrMissingCommand ...
var ErrMissingCommand = errors.New("missing command")

// ErrInvalidCommand ...
var ErrInvalidCommand = errors.New("invalid command")

// ErrMissingKey ...
var ErrMissingKey = errors.New("missing key")

// ErrMissingCRLF ...
var ErrMissingCRLF = errors.New("missing CRLF")

// ErrMissingLease ...
var ErrMissingLease = errors.New("missing lease")

// ErrLeaseNotNumber ...
var ErrLeaseNotNumber = errors.New("lease is not number")

// ErrMissingSize ...
var ErrMissingSize = errors.New("missing size")

// ErrSizeNotNumber ...
var ErrSizeNotNumber = errors.New("size is not number")

// ErrMissingData ...
var ErrMissingData = errors.New("missing data")

// Parser ...
type Parser struct {
	handler CommandHandler
	scanner scanner
}

// InitParser ...
func InitParser(p *Parser, handler CommandHandler) {
	p.handler = handler
	initScanner(&p.scanner)
}

func bytesToUint32(data []byte) uint32 {
	num := uint32(0)
	for _, n := range data {
		num *= 10
		num += uint32(n - '0')
	}
	return num
}

// Process ...
func (p *Parser) Process(data []byte) error {
	p.scanner.reset()
	p.scanner.scan(data)

	tokens := p.scanner.tokens
	if len(tokens) == 0 {
		return ErrMissingCommand
	}

	switch tokens[0].tokenType {
	case tokenTypeLGET:
		return p.processLGET(data)
	case tokenTypeLSET:
		return p.processLSET(data)
	case tokenTypeDEL:
		return p.processDEL(data)
	case tokenTypeCRLF:
		return ErrMissingCommand
	default:
		return ErrInvalidCommand
	}
}

func tokenTypeIsString(t tokenType) bool {
	switch t {
	case tokenTypeLGET, tokenTypeLSET,
		tokenTypeDEL, tokenTypeIdent, tokenTypeInt:
		return true
	default:
		return false
	}
}

func (p *Parser) processLGET(data []byte) error {
	tokens := p.scanner.tokens
	if len(tokens) < 2 || !tokenTypeIsString(tokens[1].tokenType) {
		return ErrMissingKey
	}
	if len(tokens) < 3 || tokens[2].tokenType != tokenTypeCRLF {
		return ErrMissingCRLF
	}

	p.handler.OnLGET(tokens[1].getData(data))
	return nil
}

func validateLSETControlTokens(tokens []token) error {
	if len(tokens) < 2 {
		return ErrMissingKey
	}
	if len(tokens) < 3 {
		return ErrMissingLease
	}
	if tokens[2].tokenType != tokenTypeInt {
		return ErrLeaseNotNumber
	}
	if len(tokens) < 4 {
		return ErrMissingSize
	}
	if tokens[3].tokenType != tokenTypeInt {
		return ErrSizeNotNumber
	}
	if len(tokens) < 5 || tokens[4].tokenType != tokenTypeCRLF {
		return ErrMissingCRLF
	}
	return nil
}

func (p *Parser) processLSET(data []byte) error {
	tokens := p.scanner.tokens
	err := validateLSETControlTokens(tokens)
	if err != nil {
		return err
	}

	key := tokens[1].getData(data)
	lease := bytesToUint32(tokens[2].getData(data))
	size := bytesToUint32(tokens[3].getData(data))

	beginValueOffset := tokens[4].end
	data = data[beginValueOffset:]

	if len(data) < int(size) {
		return ErrMissingData
	}

	p.scanner.reset()
	p.scanner.scanBinary(int(size), data)

	tokens = p.scanner.tokens
	if len(tokens) < 2 || tokens[1].tokenType != tokenTypeCRLF {
		return ErrMissingCRLF
	}
	value := tokens[0].getData(data)

	p.handler.OnLSET(key, lease, value)
	return nil
}

func (p *Parser) processDEL(data []byte) error {
	tokens := p.scanner.tokens
	if len(tokens) < 2 || !tokenTypeIsString(tokens[1].tokenType) {
		return ErrMissingKey
	}
	if len(tokens) < 3 || tokens[2].tokenType != tokenTypeCRLF {
		return ErrMissingCRLF
	}
	p.handler.OnDEL(tokens[1].getData(data))
	return nil
}
