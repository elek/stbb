package placement

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/db"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/common/memory"
	"storj.io/common/storj"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/overlay"
	"time"
)

type Simulate struct {
	db.WithDatabase
	Selector string
	Filter   string
	NodeNo   int `default:"110"`
	Number   int `default:"1"`
}

func (s Simulate) Run() error {
	ctx := context.Background()

	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	satelliteDB, err := s.WithDatabase.GetSatelliteDB(ctx, log)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		satelliteDB.Close()
	}()

	env := nodeselection.NewPlacementConfigEnvironment(nil, nil)
	selectorInit, err := nodeselection.SelectorFromString(s.Selector, env)
	if err != nil {
		return errors.WithStack(err)
	}

	f, err := nodeselection.FilterFromString(s.Filter, env)
	if err != nil {
		return errors.WithStack(err)
	}
	d := nodeselection.PlacementDefinitions{
		0: nodeselection.Placement{
			ID:         0,
			Selector:   selectorInit,
			NodeFilter: f,
		},
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

	nodes := map[storj.NodeID]*nodeselection.SelectedNode{}
	selected := map[storj.NodeID]int{}
	for i := 0; i < s.Number; i++ {
		selection, err := cache.GetNodes(ctx, overlay.FindStorageNodesRequest{
			RequestedCount: s.NodeNo,
			Placement:      storj.PlacementConstraint(0),
			Requester:      storj.NodeID{},
		})
		if err != nil {
			return errors.WithStack(err)
		}

		for _, node := range selection {
			selected[node.ID]++
			nodes[node.ID] = node
		}
	}
	for id, count := range selected {
		fmt.Println(id, nodes[id].Address.Address, count)
	}
	return nil
}
