package rpc

import (
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"io"
	"net"
)

type TCPClient struct {
	util.Loop
	Address string `arg:""`
}

func (c TCPClient) Run() error {
	_, err := c.Loop.Run(func() error {
		conn, err := net.Dial("tcp", c.Address)
		if err != nil {
			return err
		}
		all, err := io.ReadAll(conn)
		if err != nil {
			return err
		}
		if c.Verbose {
			fmt.Println("Downloaded", len(all), "bytes")
		}
		conn.Close()
		return nil
	})
	return err
}
