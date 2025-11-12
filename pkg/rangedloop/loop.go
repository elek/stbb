package rangedloop

import (
	"context"
	"fmt"
	"time"

	"github.com/elek/stbb/pkg/db"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"storj.io/storj/satellite/metabase/avrometabase"
	"storj.io/storj/satellite/metabase/rangedloop"
	"storj.io/storj/satellite/metrics"
)

type RangedLoop struct {
	UsedSpace   UsedSpaceLoop `cmd:"" help:"calculate used space for a given placement constraint and expiration time"`
	Checker     CheckerLoop   `cmd:"" help:"check segment health and list segments below a given normalized health threshold"`
	PieceList   PieceListLoop `cmd:"" help:"generate a piece list report for a given NodeID or a file containing a list of NodeIDs"`
	FindSegment FindSegment   `cmd:"" help:"find and export segments for a given SegmentID"`
}

type WithRangedLoop struct {
	db.WithDatabase
	ScanType  string `default:"full"`
	ScanParam int

	Parallelism int `default:"1"`

	BackupBucket string `required:"true"`
	BackupDay    string `required:"true"`
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
		segmentPattern := fmt.Sprintf("%s*/metainfo/*/segments.avro-*", r.BackupDay)
		segmentsAvroIterator := avrometabase.NewGCSIterator(r.BackupBucket, segmentPattern)
		nodeAliasPattern := fmt.Sprintf("%s*/metainfo/*/node_aliases.avro-*", r.BackupDay)
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
