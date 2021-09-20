package kvstore

import (
	"fmt"
	"net"
)

// Server ...
type Server struct {
	packageData []byte
	conn        *net.UDPConn
}

const maxPacketSize = 1 << 16

// NewServer ...
func NewServer() *Server {
	return &Server{
		packageData: make([]byte, maxPacketSize),
	}
}

// Run ...
func (s *Server) Run() error {
	addr, err := net.ResolveUDPAddr("udp", ":7000")
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	s.conn = conn

	for {
		size, addr, err := conn.ReadFrom(s.packageData)
		if err != nil {
			return err
		}
		fmt.Println(size, addr, string(s.packageData))
	}
}

func (s *Server) Shutdown() {
	_ = s.conn.Close()
}
