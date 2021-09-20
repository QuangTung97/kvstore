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
		size, addr, err := conn.ReadFromUDP(s.packageData)
		if err != nil {
			return err
		}
		fmt.Println("ReadFrom", size, addr, string(s.packageData[:size]))

		size, err = conn.WriteToUDP([]byte("Response Data"), addr)
		fmt.Println("WriteTo", size, err)
	}
}

// Shutdown ...
func (s *Server) Shutdown() error {
	return s.conn.Close()
}
