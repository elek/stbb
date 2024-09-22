package placement

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"os"
	"storj.io/common/storj"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/overlay"
	"storj.io/storj/satellite/satellitedb"
	"strings"
	"time"
)

type List struct {
	PlacementConfig string
	Placement       int
}

func (s List) Run() error {
	ctx := context.Background()

	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	d, err := nodeselection.LoadConfig(s.PlacementConfig, nodeselection.NewPlacementConfigEnvironment(nil))
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

	reputableNodes, newNodes, err := satelliteDB.OverlayCache().SelectAllStorageNodesUpload(ctx, overlay.NodeSelectionConfig{
		NewNodeFraction: 0.01,
		OnlineWindow:    4 * time.Hour,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	nodes := append(reputableNodes, newNodes...)

	slices.SortFunc(nodes, func(a, b *nodeselection.SelectedNode) int {
		return strings.Compare(a.LastIPPort, b.LastIPPort)
	})
	filter, _ := d.CreateFilters(storj.PlacementConstraint(s.Placement))
	for _, node := range nodes {
		fmt.Println(node.ID, node.Email, node.LastIPPort, node.Suspended, node.Vetted, node.Online, filter.Match(node))
		for _, tag := range node.Tags {
			fmt.Println(" ", tag.Name, string(tag.Value), tag.Signer)
		}

	}
	return nil
}
