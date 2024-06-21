package placement

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"os"
	"storj.io/common/memory"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/overlay"
	"storj.io/storj/satellite/satellitedb"
	"time"
)

type Nodes struct {
	Selector string
	Filter   string
}

func (s Nodes) Run() error {
	ctx := context.Background()

	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	var filter nodeselection.NodeFilter
	filter = nodeselection.AnyFilter{}
	if s.Filter != "" {
		filter, err = nodeselection.FilterFromString(s.Filter)
		if err != nil {
			return err
		}
	}

	satelliteDB, err := satellitedb.Open(ctx, log.Named("metabase"), os.Getenv("STBB_DB_SATELLITE"), satellitedb.Options{
		ApplicationName: "stbb",
	})
	if err != nil {
		return err
	}
	defer satelliteDB.Close()

	oldNodes, newNodes, err := satelliteDB.OverlayCache().SelectAllStorageNodesUpload(ctx, overlay.NodeSelectionConfig{
		OnlineWindow:     4 * time.Hour,
		MinimumDiskSpace: memory.GB * 5,
	})

	if err != nil {
		return errors.WithStack(err)
	}

	fmt.Println("cache is loaded", "new", len(newNodes), "old", len(oldNodes))
	selector, err := nodeselection.CreateNodeAttribute(s.Selector)
	if err != nil {
		return errors.WithStack(err)
	}
	var filtered []*nodeselection.SelectedNode
	for _, node := range append(oldNodes, newNodes...) {
		if filter.Match(node) {
			filtered = append(filtered, node)
		}
	}
	util.PrintHistogram(filtered, selector)
	fmt.Println()
	return nil
}
