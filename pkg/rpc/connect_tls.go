package rpc

import (
	"crypto/tls"
	"fmt"
	"github.com/spf13/cobra"
	"net"
	"storj.io/drpc/drpcmigrate"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:   "connect-tls <address>",
		Short: "Connect to an address with pure TCP/TLS",
		Args:  cobra.ExactArgs(1),
	}
	samples := cmd.Flags().IntP("samples", "n", 1, "Number of tests to be executed")
	sleep := cmd.Flags().IntP("sleep", "", 0, "Sleep time between milliseconds")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		conf := &tls.Config{
			InsecureSkipVerify: true,
		}

		start := time.Now()
		for i := 0; i < *samples; i++ {

			err := tlsOpenClose(args, conf)
			if err != nil {
				return err
			}
			time.Sleep(time.Duration(*sleep) * time.Millisecond)
		}

		fmt.Printf("%d\n", time.Since(start).Milliseconds()/int64(*samples))
		return nil
	}
}

func tlsOpenClose(args []string, conf *tls.Config) error {
	conn, err := net.Dial("tcp", args[0])
	if err != nil {
		return err
	}
	_, err = conn.Write([]byte(drpcmigrate.DRPCHeader))
	if err != nil {
		return err
	}

	tlsConn := tls.Client(conn, conf)
	err = tlsConn.Handshake()
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}
