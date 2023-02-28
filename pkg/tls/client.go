package tls

import (
	"crypto/tls"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs/v2"
	"log"
	"net"
	"strconv"
)

func init() {

	{
		cmd := cobra.Command{
			Use: "client <address:port> <size>",
		}
		samples := cmd.Flags().IntP("samples", "n", 1, "Number of tests to be executed")
		verbose := cmd.Flags().BoolP("verbose", "v", false, "Verbose")
		pooled := cmd.Flags().BoolP("pooled", "p", false, "Re-use original connection")
		cmd.RunE = func(cmd *cobra.Command, args []string) error {

			size, err := strconv.Atoi(args[1])
			if err != nil {
				return errs.Wrap(err)
			}

			createConnection := func() (net.Conn, error) {
				conf := &tls.Config{
					InsecureSkipVerify: true,
				}
				conn, err := tls.Dial("tcp", args[0], conf)
				if err != nil {
					return nil, err
				}
				return conn, nil
			}

			var connHandler func() (net.Conn, func(), error)
			if *pooled {
				fmt.Println("Reusing connection")
				conn, err := createConnection()
				if err != nil {
					return err
				}
				connHandler = func() (net.Conn, func(), error) {
					return conn, func() {}, nil
				}
				defer conn.Close()
			} else {
				connHandler = func() (net.Conn, func(), error) {
					conn, err := createConnection()
					return conn, func() {
						_ = conn.Close()
					}, err
				}
			}
			_, err = util.Loop(*samples, *verbose, func() error {
				return client(args[0], size, connHandler)
			})
			return err
		}
		TlsCmd.AddCommand(&cmd)
	}

}

func client(address string, size int, open func() (net.Conn, func(), error)) (err error) {
	log.SetFlags(log.Lshortfile)
	var n int
	conn, cleanup, err := open()
	if err != nil {
		return err
	}
	defer cleanup()

	_, err = conn.Write([]byte(fmt.Sprintf("%d\n", size)))
	if err != nil {
		return
	}

	buf := make([]byte, size)
	read := 0
	for read < size {
		n, err = conn.Read(buf)
		if err != nil {
			return err
		}
		read += n
	}
	return nil
}
