package tls

import (
	"bufio"
	"crypto/tls"
	stbb "github.com/elek/stbb/pkg"
	"github.com/spf13/cobra"
	"log"
	"net"
)

func init() {
	tlsCmd := cobra.Command{
		Use: "tls",
	}

	{
		cmd := cobra.Command{
			Use: "serve",
			RunE: func(cmd *cobra.Command, args []string) error {
				return run()
			},
		}

		tlsCmd.AddCommand(&cmd)
	}

	{
		cmd := cobra.Command{
			Use: "client",
			RunE: func(cmd *cobra.Command, args []string) error {
				return client()
			},
		}

		tlsCmd.AddCommand(&cmd)
	}

	stbb.RootCmd.AddCommand(&tlsCmd)
}

func run() (err error) {
	cer, err := tls.LoadX509KeyPair("server.crt", "server.key")
	if err != nil {
		return
	}

	config := &tls.Config{Certificates: []tls.Certificate{cer}}
	ln, err := tls.Listen("tcp", ":1443", config)
	if err != nil {
		return
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		msg, err := r.ReadString('\n')
		if err != nil {
			log.Println(err)
			return
		}

		println(msg)

		n, err := conn.Write([]byte("world\n"))
		if err != nil {
			log.Println(n, err)
			return
		}
	}
}
