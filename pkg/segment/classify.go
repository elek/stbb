package segment

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/elek/stbb/pkg/db"
	"github.com/elek/stbb/pkg/placement"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/repair"
	"storj.io/storj/shared/location"
)

type Classify struct {
	db.WithDatabase
	placement.WithPlacement
	StreamID       string `arg:""`
	PlacementCheck bool   `help:"Check placement rules" default:"true"`
}

func (s *Classify) Run() error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}
	ctx := context.TODO()

	metabaseDB, err := s.GetMetabaseDB(ctx, log.Named("metabase"))
	if err != nil {
		return errs.New("Error creating metabase connection: %+v", err)
	}
	defer func() {
		_ = metabaseDB.Close()
	}()

	satelliteDB, err := s.WithDatabase.GetSatelliteDB(ctx, log)
	if err != nil {
		return err
	}

	for _, streamID := range strings.Split(s.StreamID, ",") {
		su, sp, err := util.ParseSegmentPosition(streamID)
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

		nodeInfo := map[storj.NodeID]nodeselection.SelectedNode{}

		{
			nodeIDs := storj.NodeIDList{}
			for _, piece := range segment.Pieces {
				nodeIDs = append(nodeIDs, piece.StorageNode)
			}

			var selected []nodeselection.SelectedNode

			participatingNodes, err := satelliteDB.OverlayCache().GetAllParticipatingNodes(ctx, 4*time.Hour, 5*time.Minute)
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

		placements, err := s.GetPlacement(nodeselection.NewPlacementConfigEnvironment(nil, nil))
		if err != nil {
			return errs.Wrap(err)
		}
		placement = placements[segment.Placement]

		fmt.Println("segment", segment.StreamID)
		fmt.Println("placement", segment.Placement)

		result := repair.ClassifySegmentPieces(
			segment.Pieces,
			selectedNodes,
			map[location.CountryCode]struct{}{},
			s.PlacementCheck,
			s.PlacementCheck,
			placement)
		fmt.Println("redundancy", fmt.Sprintf("%d/(%d)/%d/%d", segment.Redundancy.RequiredShares, segment.Redundancy.RepairShares, segment.Redundancy.OptimalShares, segment.Redundancy.TotalShares))
		fmt.Println("repaired_at", segment.RepairedAt)
		pattern := "%-20s %d\n"
		fmt.Printf(pattern, "healthy", result.Healthy.Count())
		fmt.Printf(pattern, "forcing-repair", result.ForcingRepair.Count())
		fmt.Printf(pattern, "uhealthy", result.Unhealthy.Count())
		fmt.Printf(pattern, "in-excluded-country", result.InExcludedCountry.Count())
		fmt.Printf(pattern, "suspended", result.Suspended.Count())
		fmt.Printf(pattern, "exiting", result.Exiting.Count())
		fmt.Printf(pattern, "missing", result.Missing.Count())
		fmt.Printf(pattern, "unhealhty-retrvb.", result.UnhealthyRetrievable.Count())
		fmt.Printf(pattern, "clumped", result.Clumped.Count())
		fmt.Printf(pattern, "out-of-placement", result.OutOfPlacement.Count())

		for _, piece := range segment.Pieces {
			fmt.Printf("[%s] %d %s %s\n", getStatus(result, int(piece.Number)), piece.Number, piece.StorageNode, getNodeInfo(nodeInfo, piece.StorageNode))
		}
	}
	return nil
}

func getStatus(result repair.PiecesCheckResult, number int) string {
	st := []string{}
	if result.Healthy.Contains(number) {
		st = append(st, "healthy")
	}
	if result.Missing.Contains(number) {
		st = append(st, "missing")
	}
	if result.Clumped.Contains(number) {
		st = append(st, "clumped")
	}
	if result.Exiting.Contains(number) {
		st = append(st, "exiting")
	}
	if result.Suspended.Contains(number) {
		st = append(st, "suspended")
	}
	if result.Retrievable.Contains(number) {
		st = append(st, "retrievable")
	}
	if result.OutOfPlacement.Contains(number) {
		st = append(st, "oop")
	}

	return strings.Join(st, ",")
}

func getNodeInfo(info map[storj.NodeID]nodeselection.SelectedNode, nodeID storj.NodeID) string {

	node, found := info[nodeID]
	if !found {
		return "???"
	}
	identification := ""
	for _, tag := range []string{"host", "service"} {
		hostTag, err := node.Tags.FindBySignerAndName(nodeID, tag)
		if err == nil {
			if len(identification) > 0 {
				identification += "/"
			}
			identification += string(hostTag.Value)
		}
	}
	return fmt.Sprintf("%s %s %s", node.LastIPPort, node.Email, identification)
}
