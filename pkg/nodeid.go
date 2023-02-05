package stbb

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/spf13/cobra"
	"storj.io/common/identity"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc"
	"storj.io/common/socket"
	"storj.io/common/storj"
	"storj.io/drpc"
	"time"
)

func init() {

	nodeIDCmd := &cobra.Command{
		Use:     "nodeid",
		Aliases: []string{"nid", "n"},
	}

	nodeIDCmd.AddCommand(&cobra.Command{
		Use:   "decode",
		Short: "Decode base64 nodeid to binary",
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := storj.NodeIDFromString(args[0])
			if err != nil {
				return err
			}
			fmt.Println(hex.EncodeToString(id.Bytes()))
			return nil
		},
	})

	nodeIDCmd.AddCommand(&cobra.Command{
		Use:   "read",
		Short: "Decode base64 nodeid to binary",
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := identity.NodeIDFromCertPath(args[0])
			if err != nil {
				return err
			}
			fmt.Println(hex.EncodeToString(id.Bytes()))
			fmt.Println(id.String())
			return nil
		},
	})

	nodeIDCmd.AddCommand(&cobra.Command{
		Use:   "encode",
		Short: "encode raw nodeid to base64",
		RunE: func(cmd *cobra.Command, args []string) error {
			bs, err := hex.DecodeString(args[0])
			if err != nil {
				return err
			}

			id, err := storj.NodeIDFromBytes(bs)
			if err != nil {
				return err
			}
			fmt.Println(id.String())
			return nil
		},
	})
	nodeIDCmd.AddCommand(&cobra.Command{
		Use:   "remote",
		Short: "Get node id of remote host/port",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			id, err := GetSatelliteID(ctx, args[0])
			if err != nil {
				return err
			}
			fmt.Println(id)
			return nil
		},
	})
	RootCmd.AddCommand(nodeIDCmd)
}

// GetSatelliteID retrieves node identity from SSL endpoint.
// Only for testing. Using identified node id is not reliable.
func GetSatelliteID(ctx context.Context, address string) (string, error) {
	tlsOptions, err := getProcessTLSOptions(ctx)
	if err != nil {
		return "", err
	}

	dialer := rpc.NewDefaultDialer(tlsOptions)
	dialer.Pool = rpc.NewDefaultConnectionPool()

	dialer.DialTimeout = 10 * time.Second
	dialContext := socket.BackgroundDialer().DialContext

	//lint:ignore SA1019 it's safe to use TCP here instead of QUIC + TCP
	dialer.Connector = rpc.NewDefaultTCPConnector(&rpc.ConnectorAdapter{DialContext: dialContext}) //nolint:staticcheck

	conn, err := dialer.DialAddressInsecure(ctx, address)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	in := struct{}{}
	out := struct{}{}
	_ = conn.Invoke(ctx, "asd", &NullEncoding{}, in, out)
	peerIdentity, err := conn.PeerIdentity()
	if err != nil {
		return "", err
	}

	return peerIdentity.ID.String() + "@" + address, nil

}

func getProcessTLSOptions(ctx context.Context) (*tlsopts.Options, error) {
	ident, err := identity.NewFullIdentity(ctx, identity.NewCAOptions{
		Difficulty:  0,
		Concurrency: 1,
	})
	if err != nil {
		return nil, err
	}

	tlsConfig := tlsopts.Config{
		UsePeerCAWhitelist: false,
		PeerIDVersions:     "0",
	}

	tlsOptions, err := tlsopts.NewOptions(ident, tlsConfig, nil)
	if err != nil {
		return nil, err
	}

	return tlsOptions, nil
}

type NullEncoding struct {
}

func (n NullEncoding) Marshal(msg drpc.Message) ([]byte, error) {
	return []byte{1}, nil
}

func (n NullEncoding) Unmarshal(buf []byte, msg drpc.Message) error {
	return nil
}

var _ drpc.Encoding = &NullEncoding{}
