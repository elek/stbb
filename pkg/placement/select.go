package placement

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"os"
	"storj.io/common/memory"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/overlay"
	"storj.io/storj/satellite/satellitedb"
	"time"
)

type Select struct {
	PlacementConfig string
	Placement       int
	NodeNo          int    `default:"110"`
	Selector        string `default:"wallet"`
	Number          int    `default:"1"`
}

func (s Select) Run() error {
	ctx := context.Background()

	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	satelliteDB, err := satellitedb.Open(ctx, log.Named("metabase"), os.Getenv("STBB_DB_SATELLITE"), satellitedb.Options{
		ApplicationName: "stbb",
	})
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		satelliteDB.Close()
	}()

	d, err := nodeselection.LoadConfig(s.PlacementConfig, nodeselection.NewPlacementConfigEnvironment(nil))
	if err != nil {
		return errors.WithStack(err)
	}

	cache, err := overlay.NewUploadSelectionCache(log, satelliteDB.OverlayCache(), 60*time.Minute, overlay.NodeSelectionConfig{
		NewNodeFraction:  0.01,
		OnlineWindow:     4 * time.Hour,
		MinimumDiskSpace: 5 * memory.GB,
	}, nil, d)
	if err != nil {
		return errors.WithStack(err)
	}

	go func() {
		err = cache.Run(ctx)
		fmt.Println(err)
	}()

	start := time.Now()
	err = cache.Refresh(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Info("Node cache is loaded", zap.Duration("duration", time.Since(start)))

	for i := 0; i < s.Number; i++ {
		nodes, err := cache.GetNodes(ctx, overlay.FindStorageNodesRequest{
			RequestedCount: s.NodeNo,
			Placement:      storj.PlacementConstraint(s.Placement),
			Requester:      storj.NodeID{},
		})
		selector, err := nodeselection.CreateNodeAttribute(s.Selector)
		if err != nil {
			return errors.WithStack(err)
		}
		pieces, invNodes := convert(nodes)
		oop := d[storj.PlacementConstraint(s.Placement)].Invariant(pieces, invNodes)
		util.PrintHistogram(nodes, selector)
		fmt.Println("Out of placement nodes", oop.Count())
	}

	return nil
}

func convert(orig []*nodeselection.SelectedNode) (pieces metabase.Pieces, nodes []nodeselection.SelectedNode) {
	for ix, node := range orig {
		pieces = append(pieces, metabase.Piece{
			Number: uint16(ix),
		})
		nodes = append(nodes, *node)
	}
	return
}
