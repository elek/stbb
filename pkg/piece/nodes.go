package piece

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs"
	"os"
	"storj.io/common/grant"
	"storj.io/common/identity"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc"
	"storj.io/common/rpc/quic"
	"storj.io/storj/cmd/uplink/ulloc"
	"storj.io/uplink/private/metaclient"
)

func init() {
	cmd := &cobra.Command{
		Use:   "nodes",
		Short: "Print out storagenodes which stores a specific object",
	}
	samples := cmd.Flags().IntP("samples", "n", 1, "Number of tests to be executed")
	useQuic := cmd.Flags().BoolP("quic", "q", false, "Force to use quic protocol")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return listLocations(args[0], *samples, *useQuic)
	}
	PieceCmd.AddCommand(cmd)

}

func listLocations(s string, samples int, useQuic bool) error {
	ctx := context.Background()
	gr := os.Getenv("UPLINK_ACCESS")

	p, err := ulloc.Parse(s)
	if err != nil {
		return err
	}
	bucket, key, ok := p.RemoteParts()
	if !ok {
		return errs.New("Path is not remote %s", s)
	}

	dialer, err := getDialer(ctx, useQuic)
	if err != nil {
		return err
	}

	access, err := grant.ParseAccess(gr)
	if err != nil {
		return err
	}
	metainfoClient, err := metaclient.DialNodeURL(ctx,
		dialer,
		access.SatelliteAddress,
		access.APIKey,
		"stbb")
	if err != nil {
		return err
	}
	defer metainfoClient.Close()

	decoded, err := base64.URLEncoding.DecodeString(key)
	if err != nil {
		return err
	}

	nodes := map[string]bool{}

	for i := 0; i < samples; i++ {
		resp, err := metainfoClient.DownloadObject(ctx, metaclient.DownloadObjectParams{
			Bucket:             []byte(bucket),
			EncryptedObjectKey: decoded,
		})
		if err != nil {
			return err
		}
		for _, k := range resp.DownloadedSegments {
			for _, l := range k.Limits {
				if l != nil && l.StorageNodeAddress != nil {
					nodeID := l.Limit.StorageNodeId.String()
					if _, found := nodes[nodeID]; !found {
						fmt.Println(nodeID+"@"+l.StorageNodeAddress.Address, l.Limit.PieceId, l.Limit.Limit)
						nodes[nodeID] = true
					}
				}
			}
		}
	}

	return nil
}

func getDialer(ctx context.Context, forceQuic bool) (rpc.Dialer, error) {
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
	if forceQuic {
		dialer.Connector = quic.NewDefaultConnector(nil)
	} else {
		dialer.Connector = rpc.NewDefaultTCPConnector(nil)
	}

	return dialer, nil
}

func getTCPDialer(ctx context.Context) (rpc.Dialer, error) {
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
