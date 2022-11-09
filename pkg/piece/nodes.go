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
	"storj.io/storj/cmd/uplink/ulloc"
	"storj.io/uplink/private/metaclient"
)

func init() {
	PieceCmd.AddCommand(&cobra.Command{
		Use:   "nodes",
		Short: "Print out storagenodes which stores a specific object",
		RunE: func(cmd *cobra.Command, args []string) error {
			return listLocations(args[0])
		},
	})
}

func listLocations(s string) error {
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

	dialer, err := getDialer(ctx)
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
				fmt.Println(l.Limit.StorageNodeId.String()+"@"+l.StorageNodeAddress.Address, l.Limit.PieceId, l.Limit.Limit)
			}
		}
	}

	return nil
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
