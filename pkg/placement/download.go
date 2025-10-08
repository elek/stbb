package placement

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/elek/stbb/pkg/access"
	"github.com/elek/stbb/pkg/db"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"storj.io/storj/satellite/nodeselection"
)

type Download struct {
	util.DialerHelper
	util.Loop
	db.WithDatabase
	NodeURL      storj.NodeURL
	Bucket       string `arg:""`
	EncryptedKey string `arg:""`
	DesiredNodes int    `default:"39"`
	Selector     string `default:"tag:provider"`
	Debug        bool
}

func (n *Download) Run() (err error) {
	ctx := context.Background()
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	dialer, err := n.CreateRPCDialer()
	if err != nil {
		return errors.WithStack(err)
	}

	satelliteDB, err := n.GetSatelliteDB(ctx, log)
	if err != nil {
		return errors.WithStack(err)
	}
	nodeList, err := satelliteDB.OverlayCache().GetAllParticipatingNodes(ctx, 4*time.Hour, -10*time.Millisecond)
	if err != nil {
		return errors.WithStack(err)
	}

	nodes := map[storj.NodeID]nodeselection.SelectedNode{}
	for _, node := range nodeList {
		nodes[node.ID] = node
	}

	conn, err := dialer.DialNode(ctx, n.NodeURL, rpc.DialOptions{})
	if err != nil {
		return err
	}
	defer conn.Close()

	access, err := access.ParseAccess(os.Getenv("UPLINK_ACCESS"))
	if err != nil {
		return err
	}

	encryptedKey, err := hex.DecodeString(n.EncryptedKey)
	if err != nil {
		return errors.WithStack(err)
	}

	client := pb.NewDRPCMetainfoClient(conn)

	na, err := nodeselection.CreateNodeAttribute(n.Selector)
	if err != nil {
		return errors.WithStack(err)
	}

	object, err := client.DownloadObject(ctx, &pb.DownloadObjectRequest{
		Header: &pb.RequestHeader{
			ApiKey: access.ApiKey.SerializeRaw(),
		},
		Bucket:             []byte(n.Bucket),
		EncryptedObjectKey: encryptedKey,
		DesiredNodes:       int32(n.DesiredNodes),
	})
	if err != nil {
		return errors.WithStack(err)
	}

	for _, gs := range object.GetSegmentDownload() {
		for ix, limit := range gs.AddressedLimits {
			if limit.Limit == nil {
				continue
			}

			fmt.Println(ix, limit.Limit.StorageNodeId, na(nodes[limit.Limit.StorageNodeId]))

		}
	}

	return nil
}
