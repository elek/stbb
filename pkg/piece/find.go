package piece

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/spf13/cobra"
	"storj.io/uplink/private/metaclient"
	"strings"
)

func init() {
	cmd := &cobra.Command{
		Use:   "find <bucket> <storagenode>",
		Short: "Find piece which is store on a specific storage nodes",
		Args:  cobra.ExactArgs(2),
	}
	dh := util.NewDialerHelper(cmd.Flags())
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		p, err := NewPieceFinder(ctx, args[1], dh)
		if err != nil {
			return err
		}

		err = p.Find(ctx, args[0])
		if err != nil {
			return err
		}
		return err
	}
	PieceCmd.AddCommand(cmd)
}

type PieceFinder struct {
	Downloader
}

func NewPieceFinder(ctx context.Context, storagenodeID string, dh *util.DialerHelper) (PieceFinder, error) {
	downloader, err := NewDownloader(ctx, storagenodeID, dh)
	if err != nil {
		return PieceFinder{}, err
	}
	return PieceFinder{
		Downloader: downloader,
	}, nil
}
func (p PieceFinder) Find(ctx context.Context, bucketName string) error {

	dialer, err := p.dialer.CreateRPCDialer()
	if err != nil {
		return err
	}
	metainfoClient, err := metaclient.DialNodeURL(ctx,
		dialer,
		p.satelliteURL.String(),
		p.grant.APIKey,
		"stbb")
	if err != nil {
		return err
	}

	objects, _, err := metainfoClient.ListObjects(ctx, metaclient.ListObjectsParams{
		Bucket: []byte(bucketName),
	})
	if err != nil {
		return err
	}

	for _, o := range objects {
		ips, err := metainfoClient.GetObjectIPs(ctx, metaclient.GetObjectIPsParams{
			Bucket:             []byte(bucketName),
			EncryptedObjectKey: o.EncryptedObjectKey,
		})
		if err != nil {
			return err
		}
		for _, ip := range ips.IPPorts {
			if strings.Contains(string(ip), p.storagenodeURL.Address) {
				fmt.Println(base64.URLEncoding.EncodeToString(o.EncryptedObjectKey))
			}
		}
	}

	return nil
}
