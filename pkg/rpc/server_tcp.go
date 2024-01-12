package rpc

import (
	"fmt"
	"math/rand"
	"net"
	"time"
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
			for i := 0; i < len(randomBytes); i++ {
				_, err := conn.Write(randomBytes[i : i+1])
				if err != nil {
					fmt.Println(err)
				}
				time.Sleep(1 * time.Second)
			}
			//_, err := conn.Write(randomBytes)

			fmt.Println("Served", len(randomBytes), "bytes")
			conn.Close()
		}()
	}
}
