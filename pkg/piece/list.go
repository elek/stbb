package piece

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs"
	"os"
	"storj.io/common/grant"
	"storj.io/storj/cmd/uplink/ulloc"
	"storj.io/uplink/private/metaclient"
)

func init() {
	cmd := &cobra.Command{
		Use:   "list <sj://bucket/encodedpath>",
		Short: "Print out pieces for one particular object",
		Args:  cobra.ExactArgs(1),
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return listPieces(args[0])
	}
	PieceCmd.AddCommand(cmd)
}

func listPieces(s string) error {
	p, err := ulloc.Parse(s)
	if err != nil {
		return err
	}
	bucket, key, ok := p.RemoteParts()
	if !ok {
		return errs.New("Path is not remote %s", s)
	}

	ctx := context.Background()
	gr := os.Getenv("UPLINK_ACCESS")

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

	resp, err := metainfoClient.GetObjectIPs(ctx, metaclient.GetObjectIPsParams{
		Bucket:             []byte(bucket),
		EncryptedObjectKey: decoded,
		Version:            0,
	})
	if err != nil {
		return err
	}
	for _, k := range resp.IPPorts {
		if k != nil {
			fmt.Println(string(k))
		}
	}

	return nil
}
