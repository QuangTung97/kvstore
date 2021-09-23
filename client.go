package kvstore

import (
	"context"
	"fmt"
	"net"
)

// Client ...
type Client struct {
	conn *net.UDPConn
}

// Pipeline ...
type Pipeline struct {
	conn *net.UDPConn
}

// NewClient ...
func NewClient(addr string) (*Client, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
	}, nil
}

// Pipelined ...
func (c *Client) Pipelined(_ context.Context, fn func(pipeline *Pipeline) error) error {
	return fn(&Pipeline{conn: c.conn})
}

// Shutdown ...
func (c *Client) Shutdown() error {
	return c.conn.Close()
}

// DoSomething ...
func (p *Pipeline) DoSomething() {
	size, err := p.conn.Write([]byte("Ta Quang Tung"))
	fmt.Println("Pipeline Write", size, err)

	data := make([]byte, 1<<15)
	size, err = p.conn.Read(data)
	fmt.Println("Pipeline Read:", size, err, string(data[:size]))
}
