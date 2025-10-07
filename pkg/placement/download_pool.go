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

type DownloadPool struct {
	util.DialerHelper
	util.Loop
	db.WithDatabase
	WithPlacement
	NodeURL      storj.NodeURL
	Bucket       string `arg:""`
	EncryptedKey string `arg:""`
	DesiredNodes int    `default:"39"`
	Selector     string `default:"tag:provider"`
	Debug        bool
	Placement    int
}

func (n *DownloadPool) Run() (err error) {
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
		DesiredNodes:       1000, // get all nodes
	})
	if err != nil {
		return errors.WithStack(err)
	}

	selectedNodes := make(map[storj.NodeID]*nodeselection.SelectedNode)

	for _, gs := range object.GetSegmentDownload() {
		for _, limit := range gs.AddressedLimits {
			if limit.Limit == nil {
				continue
			}

			node := nodes[limit.Limit.StorageNodeId]
			selectedNodes[limit.Limit.StorageNodeId] = &node

		}
	}

	placements, err := n.WithPlacement.GetPlacement(nodeselection.NewPlacementConfigEnvironment(&nodeselection.NoopSuccessTracker{}, &NoopFailureTracker{}))
	if err != nil {
		return errors.WithStack(err)
	}
	placement := placements[storj.PlacementConstraint(n.Placement)]

	selections := map[string][]int{}
	_, err = n.Loop.Run(func() error {

		counters := map[string]int{}

		choosen, err := placement.DownloadSelector(ctx, storj.NodeID{}, selectedNodes, n.DesiredNodes)
		if err != nil {
			return errors.WithStack(err)
		}

		l := 0
		for _, v := range choosen {
			if l >= n.DesiredNodes {
				break
			}
			counters[na(*v)]++
			l++
		}

		for k, v := range counters {
			selections[k] = append(selections[k], v)
		}
		return nil
	})
	if err != nil {
		return errors.WithStack(err)
	}

	for k, v := range selections {
		fmt.Println(k, len(v), smax(v))
	}

	return nil
}

func smax(v []int) any {
	if len(v) == 0 {
		return 0
	}
	ret := v[0]
	for _, n := range v {
		if n > ret {
			ret = n
		}
	}
	return ret
}
