package segment

import (
	"context"
	"fmt"
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
	"strconv"
	"strings"
	"time"
)

type Classify struct {
	StreamID      string `arg:""`
	PlacementFile string
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

	sp := metabase.SegmentPosition{}
	parts := strings.Split(s.StreamID, "/")

	if len(parts) > 1 {
		part, err := strconv.Atoi(parts[1])
		if err != nil {
			return err
		}
		sp = metabase.SegmentPositionFromEncoded(uint64(part))
	}
	su, err := ParseUUID(parts[0])
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

		selected, err := satelliteDB.OverlayCache().GetNodes(ctx, nodeIDs, 30*24*time.Hour, -5*time.Minute)
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

	var filter nodeselection.NodeFilter
	{
		c := nodeselection.ConfigurablePlacementRule{
			s.PlacementFile,
		}
		def, err := c.Parse()
		if err != nil {
			return err
		}
		filter = def.CreateFilters(segment.Placement)
	}

	fmt.Println("segment", segment.StreamID)
	fmt.Println("placement", segment.Placement)

	result := repair.ClassifySegmentPieces(
		segment.Pieces,
		selectedNodes,
		map[location.CountryCode]struct{}{},
		true,
		true,
		filter,
		[]storj.NodeID{})
	pattern := "%-20s %d\n"
	fmt.Printf(pattern, "healthy", result.Healthy.Size())
	fmt.Printf(pattern, "forcing-repair", result.ForcingRepair.Size())
	fmt.Printf(pattern, "uhealthy", result.Unhealthy.Size())
	fmt.Printf(pattern, "suspended", result.Suspended.Size())
	fmt.Printf(pattern, "existing", result.Exiting.Size())
	fmt.Printf(pattern, "missing", result.Missing.Size())
	fmt.Printf(pattern, "unhealhty-retrvb.", result.UnhealthyRetrievable.Size())
	fmt.Printf(pattern, "missing", result.Missing.Size())
	fmt.Printf(pattern, "clumped", result.Clumped.Size())
	fmt.Printf(pattern, "out-of-placement", result.OutOfPlacement.Size())

	return nil
}
