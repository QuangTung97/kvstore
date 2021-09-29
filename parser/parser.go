package parser

//go:generate moq -out parser_mocks_test.go . CommandHandler

// CommandHandler ...
type CommandHandler interface {
	OnLGET(key []byte)
}

// Parser ...
type Parser struct {
	handler CommandHandler
}

// InitParser ...
func InitParser(p *Parser, handler CommandHandler) {
	p.handler = handler
}

// Process ...
func (p *Parser) Process(data []byte) {
	key := data[:len(LGET)+1]
	p.handler.OnLGET(key)
}
