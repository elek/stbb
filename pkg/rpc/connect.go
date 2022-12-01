package rpc

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"storj.io/common/identity"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:   "connect <storagenode-id> ",
		Short: "Connect to a storagenode and close the connection",
		Args:  cobra.ExactArgs(1),
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {

		ctx := context.Background()

		start := time.Now()

		dialer, err := getDialer(ctx)
		if err != nil {
			return err
		}

		storagenodeURL, err := storj.ParseNodeURL(args[0])
		if err != nil {
			return err
		}

		samples := 100
		for i := 0; i < samples; i++ {
			fmt.Println(i)
			conn, err := dialer.DialNodeURL(ctx, storagenodeURL)
			if err != nil {
				return err
			}
			conn.Close()
		}

		fmt.Printf("%d", time.Since(start).Milliseconds()/int64(samples))
		return nil
	}
	RpcCmd.AddCommand(cmd)
}

func getDialer(ctx context.Context) (rpc.Dialer, error) {
	ident, err := identity.NewFullIdentity(ctx, identity.NewCAOptions{
		Difficulty:  0,
		Concurrency: 1,
	})
	if err != nil {
		return rpc.Dialer{}, err
	}

	tlsConfig := tlsopts.Config{
		UsePeerCAWhitelist: false,
		PeerIDVersions:     "0",
	}

	tlsOptions, err := tlsopts.NewOptions(ident, tlsConfig, nil)
	if err != nil {
		return rpc.Dialer{}, err
	}
	dialer := rpc.NewDefaultDialer(tlsOptions)
	dialer.Connector = rpc.NewDefaultTCPConnector(nil)
	return dialer, nil
}
