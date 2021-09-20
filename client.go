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
func (c *Client) Pipelined(ctx context.Context, fn func(pipeline *Pipeline) error) error {
	return fn(&Pipeline{conn: c.conn})
}

func (c *Client) Shutdown() {
	err := c.conn.Close()
	fmt.Println(err)
}

// DoSomething ...
func (p *Pipeline) DoSomething() {
	size, err := p.conn.Write([]byte("Ta Quang Tung"))
	fmt.Println(size, err)
}
