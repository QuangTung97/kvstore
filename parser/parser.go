package parser

//go:generate moq -out parser_mocks_test.go . CommandHandler

// CommandHandler ...
type CommandHandler interface {
	OnLGET(key []byte)
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

// Process ...
func (p *Parser) Process(data []byte) {
	p.scanner.scan(data)
	p.handler.OnLGET(p.scanner.tokens[1].getData(data))
}
