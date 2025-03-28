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
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/overlay"
	"storj.io/storj/satellite/satellitedb"
	"time"
)

type Nodes struct {
	WithPlacement
	Selector []string
	Filter   string

	Placement          int
	OnlineWindow       time.Duration `default:"4h"`
	MinimumDiskSpace   memory.Size   `default:"500GB"`
	IgnoreUploadFilter bool
}

func (s Nodes) Run() error {
	ctx := context.Background()

	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	var filter, uploadFilter nodeselection.NodeFilter
	filter = nodeselection.AnyFilter{}
	uploadFilter = nodeselection.AnyFilter{}

	if s.PlacementConfig != "" {
		d, err := s.WithPlacement.GetPlacement(nodeselection.NewPlacementConfigEnvironment(nil, nil))
		if err != nil {
			return errors.WithStack(err)
		}
		uploadFilter = d[storj.PlacementConstraint(s.Placement)].UploadFilter
		filter = d[storj.PlacementConstraint(s.Placement)].NodeFilter
	}
	if s.Filter != "" {
		f, err := nodeselection.FilterFromString(s.Filter, nil)
		if err != nil {
			return err
		}
		filter = nodeselection.NodeFilters{
			f,
			filter,
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
		OnlineWindow:     s.OnlineWindow,
		MinimumDiskSpace: s.MinimumDiskSpace,
	})

	if err != nil {
		return errors.WithStack(err)
	}

	fmt.Println("cache is loaded", "new", len(newNodes), "old", len(oldNodes))

	var attr []nodeselection.NodeAttribute
	for _, s := range s.Selector {
		selector, err := nodeselection.CreateNodeAttribute(s)
		if err != nil {
			return errors.WithStack(err)
		}
		attr = append(attr, selector)
	}
	var filtered []*nodeselection.SelectedNode
	for _, node := range append(oldNodes, newNodes...) {
		if filter != nil && !filter.Match(node) {
			continue
		}
		if uploadFilter != nil && !uploadFilter.Match(node) && !s.IgnoreUploadFilter {
			continue
		}
		filtered = append(filtered, node)

	}
	util.PrintHistogram(filtered, attr...)
	fmt.Println()
	return nil
}
