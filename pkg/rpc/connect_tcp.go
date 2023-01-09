package rpc

import (
	"fmt"
	"github.com/spf13/cobra"
	"net"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:   "connect-tcp <address>",
		Short: "Connect to an address with pure TCP",
		Args:  cobra.ExactArgs(1),
	}
	samples := cmd.Flags().IntP("samples", "n", 1, "Number of tests to be executed")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {

		start := time.Now()

		for i := 0; i < *samples; i++ {
			dial, err := net.Dial("tcp", args[0])
			if err != nil {
				return err
			}
			dial.Close()
		}
		fmt.Printf("%d", time.Since(start).Milliseconds()/int64(*samples))
		return nil
	}
	RpcCmd.AddCommand(cmd)
}
