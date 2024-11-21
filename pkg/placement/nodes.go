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
	"strconv"
	"strings"
	"time"
)

type Nodes struct {
	Selector         string
	Filter           string
	OnlineWindow     time.Duration `default:"4h"`
	MinimumDiskSpace memory.Size   `default:"500GB"`
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
		if strings.Contains(s.Filter, "#") {
			placementFile, placementID, _ := strings.Cut(s.Filter, "#")
			p, err := strconv.Atoi(placementID)
			if err != nil {
				return errors.WithStack(err)
			}
			config, err := nodeselection.LoadConfig(placementFile, &nodeselection.PlacementConfigEnvironment{})
			if err != nil {
				return errors.WithStack(err)
			}
			filter = config[storj.PlacementConstraint(p)].NodeFilter
		} else {
			filter, err = nodeselection.FilterFromString(s.Filter, nil)
			if err != nil {
				return err
			}
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
