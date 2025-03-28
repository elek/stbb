package segment

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/db"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"os"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/overlay"
	"time"
)

type Histogram struct {
	db.WithDatabase
	Selector string
	StreamID string `arg:""`
}

func (h Histogram) Run() error {
	ctx := context.Background()
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	selector, err := nodeselection.CreateNodeAttribute(h.Selector)
	if err != nil {
		return errors.WithStack(err)
	}

	satelliteDB, err := h.WithDatabase.GetSatelliteDB(ctx, log)
	if err != nil {
		return err
	}
	defer satelliteDB.Close()

	metabaseDB, err := metabase.Open(ctx, log.Named("metabase"), os.Getenv("STBB_DB_METAINFO"), metabase.Config{
		ApplicationName: "stbb",
	})
	if err != nil {
		return errs.New("Error creating metabase connection: %+v", err)
	}
	defer func() {
		_ = metabaseDB.Close()
	}()

	n1, n2, err := satelliteDB.OverlayCache().SelectAllStorageNodesUpload(ctx, overlay.NodeSelectionConfig{
		OnlineWindow: 4 * time.Hour,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	nodes := append(n1, n2...)

	fmt.Println("node cache is loaded", len(nodes))

	su, sp, err := util.ParseSegmentPosition(h.StreamID)
	if err != nil {
		return err
	}
	segment, err := metabaseDB.GetSegmentByPosition(ctx, metabase.GetSegmentByPosition{
		StreamID: su,
		Position: sp,
	})
	if err != nil {
		return err
	}

	selectedNodes := make([]*nodeselection.SelectedNode, 0)
	for _, piece := range segment.Pieces {
		node := findNode(nodes, piece.StorageNode)
		if node != nil {
			selectedNodes = append(selectedNodes, node)
		} else {
			fmt.Println("Missing node", piece.StorageNode, "pieceId=", piece.Number)
		}
	}
	util.PrintHistogram(selectedNodes, selector)
	PrintBusFactor(selectedNodes, selector, 54-29)
	return nil
}

func PrintBusFactor(nodes []*nodeselection.SelectedNode, selector nodeselection.NodeAttribute, threshold int) {
	var busFactorGroups []int

	controlledByClass := map[string]int{}
	for _, n := range nodes {
		c := selector(*n)
		controlledByClass[c] = controlledByClass[c] + 1
	}

	for classID, count := range controlledByClass {
		if count == 0 {
			continue
		}

		// reset the value for the next iteration
		controlledByClass[classID] = 0

		busFactorGroups = append(busFactorGroups, count)
	}

	slices.SortFunc(busFactorGroups, func(a int, b int) int {
		return int(b - a)
	})
	rollingSum := 0
	busFactor := 0
	for _, count := range busFactorGroups {
		if rollingSum < threshold {
			busFactor++
			rollingSum += int(count)
		} else {
			break
		}
	}
	fmt.Println("bus_factor", busFactor)
}

func findNode(nodes []*nodeselection.SelectedNode, target storj.NodeID) *nodeselection.SelectedNode {
	for _, node := range nodes {
		if node.ID == target {
			return node
		}
	}
	return nil
}
