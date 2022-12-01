package piece

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs"
	"io"
	"os"
	"sort"
	"storj.io/common/grant"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"storj.io/storj/cmd/uplink/ulloc"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/piecestore"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:   "node-speed",
		Short: "Measure raw node speed performance with downloading one piece",
	}
	samples := cmd.Flags().IntP("samples", "n", 1, "Number of tests to be executed")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return nodeSpeed(args[0], *samples)
	}
	PieceCmd.AddCommand(cmd)

}

func nodeSpeed(s string, samples int) error {
	ctx := context.Background()

	dialer, err := getDialer(ctx)
	if err != nil {
		return err
	}

	nodes, err := collectNodes(ctx, dialer, s)
	if err != nil {
		return err
	}

	var sorted []string
	for k, _ := range nodes {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	for _, k := range sorted {
		downloadInfo := nodes[k]

		start := time.Now()
		d, err := downloadPiece(ctx, dialer, downloadInfo)
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Printf("%s %d %d\n", downloadInfo.Limit.StorageNodeId.String(), d, time.Since(start).Milliseconds())

	}

	return nil
}

func downloadPiece(ctx context.Context, dialer rpc.Dialer, d downloadInfo) (int64, error) {
	config := piecestore.DefaultConfig
	//config.DownloadBufferSize = 1024 * 1024
	//config.InitialStep = 1024 * 1024
	//config.MaximumStep = 1024 * 1024
	client, err := piecestore.Dial(ctx, dialer, storj.NodeURL{ID: d.Limit.StorageNodeId, Address: d.NodeAddress.Address}, config)
	if err != nil {
		return 0, err
	}
	defer client.Close()

	download, err := client.Download(ctx, d.Limit, d.PrivateKey, 0, d.Limit.Limit)
	if err != nil {
		return 0, err
	}
	defer download.Close()

	buf := bytes.Buffer{}
	downloaded, err := io.Copy(&buf, download)
	if err != nil {
		return 0, err
	}
	return downloaded, nil
}

type downloadInfo struct {
	Limit       *pb.OrderLimit
	NodeAddress *pb.NodeAddress
	PrivateKey  storj.PiecePrivateKey
}

func collectNodes(ctx context.Context, dialer rpc.Dialer, s string) (orderLimits map[string]downloadInfo, err error) {
	p, err := ulloc.Parse(s)
	if err != nil {
		return
	}
	bucket, key, ok := p.RemoteParts()
	if !ok {
		err = errs.New("Path is not remote %s", s)
		return
	}

	gr := os.Getenv("UPLINK_ACCESS")
	access, err := grant.ParseAccess(gr)
	if err != nil {
		return
	}

	metainfoClient, err := metaclient.DialNodeURL(ctx,
		dialer,
		access.SatelliteAddress,
		access.APIKey,
		"stbb")
	if err != nil {
		return
	}
	defer metainfoClient.Close()

	decoded, err := base64.URLEncoding.DecodeString(key)
	if err != nil {
		return
	}

	orderLimits = map[string]downloadInfo{}

	for i := 0; i < 20; i++ {
		resp, err := metainfoClient.DownloadObject(ctx, metaclient.DownloadObjectParams{
			Bucket:             []byte(bucket),
			EncryptedObjectKey: decoded,
		})
		if err != nil {
			return orderLimits, err
		}
		for _, segment := range resp.DownloadedSegments {
			for _, l := range segment.Limits {
				if l != nil && l.StorageNodeAddress != nil {
					nodeID := l.Limit.StorageNodeId.String()
					if _, found := orderLimits[nodeID]; !found {
						orderLimits[nodeID] = downloadInfo{
							PrivateKey:  segment.Info.PiecePrivateKey,
							Limit:       l.Limit,
							NodeAddress: l.StorageNodeAddress,
						}
					}
				}
			}
		}
	}
	return
}
