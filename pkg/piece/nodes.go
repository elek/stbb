package piece

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/zeebo/errs"
	"os"
	"storj.io/common/grant"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/storj/cmd/uplink/ulloc"
)

type Nodes struct {
	util.DialerHelper
	Path         string `arg:"" help:"Key url (sj://bucket/encryptedpath)"`
	DesiredNodes int
}

func (n *Nodes) Run() error {
	return n.OnEachNode(func(url storj.NodeURL, id storj.PieceID, size int64) error {
		fmt.Printf("%s %s %d\n", url, id, size)
		return nil
	})
}

func (n *Nodes) OnEachNode(f func(url storj.NodeURL, id storj.PieceID, size int64) error) error {
	ctx := context.Background()
	gr := os.Getenv("UPLINK_ACCESS")

	p, err := ulloc.Parse(n.Path)
	if err != nil {
		return err
	}
	bucket, key, ok := p.RemoteParts()
	if !ok {
		return errs.New("Path is not remote %s", n.Path)
	}

	dialer, err := n.CreateRPCDialer()
	if err != nil {
		return err
	}

	access, err := grant.ParseAccess(gr)
	if err != nil {
		return err
	}

	satelliteURL, err := storj.ParseNodeURL(access.SatelliteAddress)
	if err != nil {
		return err
	}

	conn, err := dialer.DialNodeURL(ctx, satelliteURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewDRPCMetainfoClient(conn)

	decoded, err := base64.URLEncoding.DecodeString(key)
	if err != nil {
		return err
	}

	nodes := map[string]bool{}

	resp, err := client.DownloadObject(ctx, &pb.DownloadObjectRequest{
		Bucket:             []byte(bucket),
		EncryptedObjectKey: decoded,
		DesiredNodes:       int32(n.DesiredNodes),
		Header: &pb.RequestHeader{
			ApiKey: access.APIKey.SerializeRaw(),
		},
	})
	if err != nil {
		return err
	}

	for _, k := range resp.GetSegmentDownload() {
		for _, l := range k.AddressedLimits {
			if l != nil && l.StorageNodeAddress != nil {
				if _, found := nodes[l.Limit.StorageNodeId.String()]; !found {
					nodeURL := storj.NodeURL{
						ID:        l.Limit.StorageNodeId,
						Address:   l.StorageNodeAddress.Address,
						NoiseInfo: l.StorageNodeAddress.NoiseInfo.Convert(),
					}
					err = f(nodeURL, l.Limit.PieceId, l.Limit.Limit)
					if err != nil {
						return err
					}
					nodes[l.Limit.StorageNodeId.String()] = true
				}
			}
		}
	}

	return nil
}
