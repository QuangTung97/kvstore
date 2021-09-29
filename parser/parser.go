package parser

//go:generate moq -out parser_mocks_test.go . CommandHandler

// CommandHandler ...
type CommandHandler interface {
	OnLGET(key []byte)
	OnLSET(key []byte, lease uint32, value []byte)
	OnDEL(key []byte)
}

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
func (p *Parser) Process(data []byte) {
	p.scanner.reset()
	p.scanner.scan(data)

	tokens := p.scanner.tokens
	switch tokens[0].tokenType {
	case tokenTypeLGET:
		p.processLGET(data)
	case tokenTypeLSET:
		p.processLSET(data)
	case tokenTypeDEL:
		p.processDEL(data)
	}
}

func (p *Parser) processLGET(data []byte) {
	tokens := p.scanner.tokens
	p.handler.OnLGET(tokens[1].getData(data))
}

func (p *Parser) processLSET(data []byte) {
	tokens := p.scanner.tokens

	key := tokens[1].getData(data)
	lease := bytesToUint32(tokens[2].getData(data))
	size := bytesToUint32(tokens[3].getData(data))

	beginValueOffset := tokens[4].end
	data = data[beginValueOffset:]
	p.scanner.reset()
	p.scanner.scanBinary(int(size), data)
	value := p.scanner.tokens[0].getData(data)

	p.handler.OnLSET(key, lease, value)
}

func (p *Parser) processDEL(data []byte) {
	tokens := p.scanner.tokens
	p.handler.OnDEL(tokens[1].getData(data))
}
