package tls

import (
	"bufio"
	"crypto/tls"
	"github.com/elek/stbb/pkg/piece"
	"github.com/spf13/cobra"
	"log"
	"net"
	"strconv"
	"strings"
)

func init() {
	{
		cmd := cobra.Command{
			Use: "serve",
			RunE: func(cmd *cobra.Command, args []string) error {
				return run()
			},
		}

		TlsCmd.AddCommand(&cmd)
	}
}

func run() (err error) {
	cer, err := tls.X509KeyPair(piece.Cert, piece.Key)
	if err != nil {
		return
	}

	config := &tls.Config{Certificates: []tls.Certificate{cer}}
	ln, err := tls.Listen("tcp", ":28967", config)
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
		length, err := strconv.Atoi(strings.TrimSpace(msg))
		if err != nil {
			log.Println(err)
			return
		}

		data := make([]byte, length)
		n, err := conn.Write(data)
		if err != nil {
			log.Println(n, err)
			return
		}
		conn.Close()
	}
}
