package rangedloop

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/db"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"os"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase/rangedloop"
	"storj.io/storj/satellite/metrics"
	"strings"
	"time"
)

type RangedLoop struct {
	db.WithDatabase
	ScanType  string `default:"full"`
	ScanParam int

	Parallelism int `default:"1"`
	NodeID      string
	Output      string

	BackupBucket   string
	BackupDatabase string
	BackupDay      string
	Instance       string
}

func (r RangedLoop) parseNodeIDs() ([]storj.NodeID, error) {
	// Check if NodeID parameter points to a file
	if fileInfo, err := os.Stat(r.NodeID); err == nil && !fileInfo.IsDir() {
		// Read nodeIDs from file
		content, err := os.ReadFile(r.NodeID)
		if err != nil {
			return nil, errs.New("Error reading nodeIDs file %s: %+v", r.NodeID, err)
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
				return nil, errs.New("Invalid nodeID in file %s: %s: %+v", r.NodeID, line, err)
			}
			nodeIDs = append(nodeIDs, nodeID)
		}
		return nodeIDs, nil
	} else {
		// Try to parse as single nodeID
		nodeID, err := storj.NodeIDFromString(r.NodeID)
		if err != nil {
			return nil, errs.New("Invalid nodeID %s: %+v", r.NodeID, err)
		}
		return []storj.NodeID{nodeID}, nil
	}
}

func (r RangedLoop) Run() error {
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

	var observers []rangedloop.Observer

	observers = append(observers, NewCount())
	observers = append(observers, metrics.NewObserver())
	if r.NodeID != "" {
		nodeIDs, err := r.parseNodeIDs()
		if err != nil {
			return err
		}
		observers = append(observers, NewPieceList(nodeIDs))
	}
	var provider rangedloop.RangeSplitter

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
		segmentsAvroIterator := rangedloop.NewAvroGCSIterator(r.BackupBucket, segmentPattern)
		metainfoPattern := fmt.Sprintf("%s*/%s/%s-*/node_aliases.avro-*", r.BackupDay, "metainfo", r.Instance)
		fmt.Println("Reading backup from", r.BackupBucket, segmentPattern, metainfoPattern)
		nodeAliasesAvroIterator := rangedloop.NewAvroGCSIterator(r.BackupBucket, metainfoPattern)
		provider = rangedloop.NewAvroSegmentsSplitter(segmentsAvroIterator, nodeAliasesAvroIterator)
	}
	service := rangedloop.NewService(log.Named("rangedloop"), cfg, provider, observers)
	_, err = service.RunOnce(ctx)
	if err != nil {
		return err
	}
	return nil
}
