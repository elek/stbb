package rpc

import (
	"fmt"
	"math/rand"
	"net"
)

type TCPServer struct {
	Address string `arg:""`
	Size    int    `arg:""`
}

func (s TCPServer) Run() error {
	randomBytes := make([]byte, s.Size)
	rand.Read(randomBytes)

	ln, err := net.Listen("tcp", s.Address)
	if err != nil {
		fmt.Println("Error listening: ", err.Error())
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
		}

		go func() {
			_, err := conn.Write(randomBytes)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("Served", len(randomBytes), "bytes")
			conn.Close()
		}()
	}
}
