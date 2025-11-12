package rangedloop

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/elek/stbb/pkg/db"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"storj.io/common/storj"
	"storj.io/storj/satellite/durability"
	"storj.io/storj/satellite/metabase/avrometabase"
	"storj.io/storj/satellite/metabase/rangedloop"
	"storj.io/storj/satellite/metrics"
)

type RangedLoop struct {
	UsedSpace UsedSpaceLoop `cmd:"" help:"calculate used space for a given placement constraint and expiration time"`
	Checker   CheckerLoop   `cmd:"" help:"check segment health and list segments below a given normalized health threshold"`
	PieceList PieceListLoop `cmd:"" help:"generate a piece list report for a given NodeID or a file containing a list of NodeIDs"`
}

type WithRangedLoop struct {
	db.WithDatabase
	ScanType  string `default:"full"`
	ScanParam int

	Parallelism int `default:"1"`

	BackupBucket   string
	BackupDatabase string
	BackupDay      string
	Instance       string
}

func (r WithRangedLoop) RunLoop(fn func(observers []rangedloop.Observer) []rangedloop.Observer) error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return err
	}

	ctx := context.Background()

	cfg := rangedloop.Config{
		Parallelism:        r.Parallelism,
		BatchSize:          2500,
		AsOfSystemInterval: -10 * time.Second,
	}

	var provider rangedloop.RangeSplitter
	var observers []rangedloop.Observer

	observers = append(observers, NewCount())
	observers = append(observers, metrics.NewObserver())
	observers = fn(observers)

	switch r.ScanType {
	case "test", "placement", "single":
		metabaseDB, err := r.GetMetabaseDB(ctx, log.Named("metabase"))
		if err != nil {
			return errs.New("Error creating metabase connection: %+v", err)
		}
		defer func() {
			_ = metabaseDB.Close()
		}()
		provider = NewFullScan(metabaseDB, r.ScanType)
	case "full":
		metabaseDB, err := r.GetMetabaseDB(ctx, log.Named("metabase"))
		if err != nil {
			return errs.New("Error creating metabase connection: %+v", err)
		}
		defer func() {
			_ = metabaseDB.Close()
		}()

		provider = rangedloop.NewMetabaseRangeSplitter(log, metabaseDB, cfg)
	case "avro":
		segmentPattern := fmt.Sprintf("%s*/%s/%s-*/segments.avro-*", r.BackupDay, r.BackupDatabase, r.Instance)
		segmentsAvroIterator := avrometabase.NewGCSIterator(r.BackupBucket, segmentPattern)
		nodeAliasPattern := fmt.Sprintf("%s*/%s/%s-*/node_aliases.avro-*", r.BackupDay, "metainfo", r.Instance)
		fmt.Println("Reading backup from", r.BackupBucket, segmentPattern, nodeAliasPattern)
		nodeAliasesAvroIterator := avrometabase.NewGCSIterator(r.BackupBucket, nodeAliasPattern)
		provider = rangedloop.NewAvroSegmentsSplitter(segmentsAvroIterator, nodeAliasesAvroIterator)
	}
	service := rangedloop.NewService(log.Named("rangedloop"), cfg, provider, observers)
	_, err = service.RunOnce(ctx)
	if err != nil {
		return err
	}
	return nil
}

type UsedSpaceLoop struct {
	WithRangedLoop
	Placement  *storj.PlacementConstraint `help:"set a placement constraint to calculate used space for"`
	Expiration time.Time                  `help:"set an expiration time to limit the scan to segments expiring before this time, format: 2006-01-02T15:04:05Z07:00"`
}

func (u UsedSpaceLoop) Run() error {
	return u.RunLoop(func(observers []rangedloop.Observer) []rangedloop.Observer {
		return append(observers, NewUsedSpace(*u.Placement, u.Expiration))
	})
}

type CheckerLoop struct {
	WithRangedLoop
	CheckerThreshold *int `help:"set a normalized health threshold to do a durability check"`
}

func (c CheckerLoop) Run() error {
	ctx := context.Background()
	log, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	satelliteDB, err := c.WithDatabase.GetSatelliteDB(ctx, log)
	if err != nil {
		return err
	}
	selectedNodes, err := satelliteDB.OverlayCache().GetAllParticipatingNodes(ctx, 4*time.Hour, -100*time.Millisecond)
	if err != nil {
		return errors.WithStack(err)
	}
	return c.RunLoop(func(observers []rangedloop.Observer) []rangedloop.Observer {
		return append(observers, NewChecker(selectedNodes, *c.CheckerThreshold))
	})
}

type PieceListLoop struct {
	WithRangedLoop
	NodeID string `help:"set a NodeID to generate a piece list report"`
}

func (p PieceListLoop) Run() error {
	nodeIDs, err := p.parseNodeIDs()
	if err != nil {
		return err
	}
	return p.RunLoop(func(observers []rangedloop.Observer) []rangedloop.Observer {
		return append(observers, NewPieceList(nodeIDs))
	})
}

func (p PieceListLoop) parseNodeIDs() ([]storj.NodeID, error) {
	// Check if NodeID parameter points to a file
	if fileInfo, err := os.Stat(p.NodeID); err == nil && !fileInfo.IsDir() {
		// Read nodeIDs from file
		content, err := os.ReadFile(p.NodeID)
		if err != nil {
			return nil, errs.New("Error reading nodeIDs file %s: %+v", p.NodeID, err)
		}

		var nodeIDs []storj.NodeID
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue // Skip empty lines and comments
			}
			nodeID, err := storj.NodeIDFromString(line)
			if err != nil {
				return nil, errs.New("Invalid nodeID in file %s: %s: %+v", p.NodeID, line, err)
			}
			nodeIDs = append(nodeIDs, nodeID)
		}
		return nodeIDs, nil
	} else {
		// Try to parse as single nodeID
		nodeID, err := storj.NodeIDFromString(p.NodeID)
		if err != nil {
			return nil, errs.New("Invalid nodeID %s: %+v", p.NodeID, err)
		}
		return []storj.NodeID{nodeID}, nil
	}
}

type DurabilityLoop struct {
	WithRangedLoop
}

func (d DurabilityLoop) Run() error {
	ctx := context.Background()
	log, err := zap.NewDevelopment()
	if err != nil {
		errors.WithStack(err)
	}
	sdb, err := d.WithDatabase.GetSatelliteDB(ctx, log.Named("satellitedb"))
	if err != nil {
		errors.WithStack(err)
	}
	mdb, err := d.WithDatabase.GetMetabaseDB(ctx, log.Named("satellitedb"))
	if err != nil {
		errors.WithStack(err)
	}
	f := durability.NewDurability(sdb.OverlayCache(), mdb, nil, "class", nil, -10*time.Second)

	return d.RunLoop(func(observers []rangedloop.Observer) []rangedloop.Observer {
		return append(observers, f)
	})
}
