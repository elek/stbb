package segment

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"os"
	"storj.io/common/storj"
	"storj.io/common/storj/location"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/repair"
	"storj.io/storj/satellite/satellitedb"
	"time"
)

type Classify struct {
	StreamID              string `arg:""`
	PlacementFile         string
	UseParticipatingNodes bool
}

func (s *Classify) Run() error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}
	ctx := context.TODO()

	metabaseDB, err := metabase.Open(ctx, log.Named("metabase"), os.Getenv("STBB_DB_METAINFO"), metabase.Config{
		ApplicationName: "stbb",
	})
	if err != nil {
		return errs.New("Error creating metabase connection: %+v", err)
	}
	defer func() {
		_ = metabaseDB.Close()
	}()

	satelliteDB, err := satellitedb.Open(ctx, log.Named("satellitedb"), os.Getenv("STBB_DB_SATELLITE"), satellitedb.Options{
		ApplicationName: "stbb",
	})
	if err != nil {
		return err
	}

	su, sp, err := util.ParseSegmentPosition(s.StreamID)
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

	selectedNodes := make([]nodeselection.SelectedNode, len(segment.Pieces))

	{
		nodeInfo := map[storj.NodeID]nodeselection.SelectedNode{}
		nodeIDs := storj.NodeIDList{}
		for _, piece := range segment.Pieces {
			nodeIDs = append(nodeIDs, piece.StorageNode)
		}

		var selected []nodeselection.SelectedNode
		if s.UseParticipatingNodes {
			participatingNodes, err := satelliteDB.OverlayCache().GetParticipatingNodes(ctx, 4*time.Hour, 5*time.Minute)
			for _, n := range participatingNodes {
				for _, id := range nodeIDs {
					if id == n.ID {
						selected = append(selected, n)
					}
				}
			}
			if err != nil {
				return err
			}
		} else {
			selected, err = satelliteDB.OverlayCache().GetNodes(ctx, nodeIDs, 4*time.Hour, -5*time.Minute)
			if err != nil {
				return err
			}
		}

		for _, sn := range selected {
			nodeInfo[sn.ID] = sn
		}

		for i, piece := range segment.Pieces {
			if sn, found := nodeInfo[piece.StorageNode]; found {
				selectedNodes[i] = sn
			}
		}
	}

	var placement nodeselection.Placement
	doPlacementCheck := false
	if s.PlacementFile != "" {
		doPlacementCheck = true
		c := nodeselection.ConfigurablePlacementRule{
			s.PlacementFile,
		}
		def, err := c.Parse(func() (nodeselection.Placement, error) {
			panic("default placement shouldn't be used")
		}, nodeselection.NewPlacementConfigEnvironment(nil))
		if err != nil {
			return errs.Wrap(err)
		}
		placement = def[segment.Placement]
	}

	fmt.Println("segment", segment.StreamID)
	fmt.Println("placement", segment.Placement)

	result := repair.ClassifySegmentPieces(
		segment.Pieces,
		selectedNodes,
		map[location.CountryCode]struct{}{},
		doPlacementCheck,
		doPlacementCheck,
		placement)
	pattern := "%-20s %d\n"
	fmt.Printf(pattern, "healthy", result.Healthy.Count())
	fmt.Printf(pattern, "forcing-repair", result.ForcingRepair.Count())
	fmt.Printf(pattern, "uhealthy", result.Unhealthy.Count())
	fmt.Printf(pattern, "in-excluded-country", result.InExcludedCountry.Count())
	fmt.Printf(pattern, "suspended", result.Suspended.Count())
	fmt.Printf(pattern, "exiting", result.Exiting.Count())
	fmt.Printf(pattern, "missing", result.Missing.Count())
	for _, piece := range segment.Pieces {
		if result.Missing.Contains(int(piece.Number)) {
			fmt.Println("   ", piece.StorageNode)
		}
	}
	fmt.Printf(pattern, "unhealhty-retrvb.", result.UnhealthyRetrievable.Count())
	fmt.Printf(pattern, "clumped", result.Clumped.Count())
	fmt.Printf(pattern, "out-of-placement", result.OutOfPlacement.Count())

	return nil
}
