package placement

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/elek/stbb/pkg/db"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"storj.io/common/storj"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/overlay"
)

type List struct {
	db.WithDatabase
	WithPlacement
	Placement  int      `help:"placement to use"`
	Attributes []string `help:"node attributes to print out"`
	Filter     string   `help:"additional display only node filter"`
}

func (s List) Run() error {
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
	var attributes []NamedNodeAttribute
	for _, attr := range s.Attributes {
		n, err := nodeselection.CreateNodeAttribute(attr)
		if err != nil {
			return err
		}
		attributes = append(attributes, NamedNodeAttribute{
			Attribute: n,
			Name:      attr,
		})
	}

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	d, err := s.WithPlacement.GetPlacement(nodeselection.NewPlacementConfigEnvironment(nil, nil))
	if err != nil {
		return errors.WithStack(err)
	}
	filter, _ := d.CreateFilters(storj.PlacementConstraint(s.Placement))

	row := table.Row{}
	row = append(row, "node_id")
	for _, attr := range attributes {
		row = append(row, attr.Name)
	}
	t.AppendHeader(row)

	if s.Filter != "" {
		additionalFilter, err := nodeselection.FilterFromString(s.Filter, nil)
		if err != nil {
			return err
		}
		filter = nodeselection.NodeFilters{
			filter,
			additionalFilter,
		}
	}

	for _, node := range nodes {
		if !filter.Match(node) {
			continue
		}
		row := table.Row{}
		row = append(row, node.ID)
		for _, attr := range attributes {
			row = append(row, attr.Attribute(*node))
		}
		t.AppendRow(row)

	}
	t.Render()
	return nil
}

type NamedNodeAttribute struct {
	Name      string
	Attribute nodeselection.NodeAttribute
}
