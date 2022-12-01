package rpc

import (
	"crypto/tls"
	"fmt"
	"github.com/spf13/cobra"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:   "connect-tls <address>",
		Short: "Connect to an address with pure TCP/TLS",
		Args:  cobra.ExactArgs(1),
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		conf := &tls.Config{
			InsecureSkipVerify: true,
		}

		start := time.Now()
		samples := 10
		for i := 0; i < samples; i++ {

			conn, err := tls.Dial("tcp", args[0], conf)
			if err != nil {
				return err
			}
			fmt.Println(i)
			conn.Close()
		}

		fmt.Printf("%d", time.Since(start).Milliseconds()/int64(samples))
		return nil
	}
	RpcCmd.AddCommand(cmd)
}
