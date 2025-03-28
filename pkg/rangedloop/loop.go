package rangedloop

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/db"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"os"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/metabase/rangedloop"
	"storj.io/storj/satellite/metrics"
	"time"
)

type RangedLoop struct {
	db.WithDatabase
	ScanType  string `default:"full"`
	ScanParam int

	Parallelism int `default:"1"`
	NodeID      storj.NodeID
	Output      string

	BackupBucket   string
	BackupDatabase string
	BackupDay      string
	Instance       string
}

func (r RangedLoop) Run() error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return err
	}

	ctx := context.Background()

	metabaseDB, err := metabase.Open(ctx, log.Named("metabase"), os.Getenv("STBB_DB_METAINFO"), metabase.Config{
		ApplicationName: "stbb",
	})
	if err != nil {
		return errs.New("Error creating metabase connection: %+v", err)
	}
	defer func() {
		_ = metabaseDB.Close()
	}()

	list, err := metabaseDB.LatestNodesAliasMap(ctx)
	if err != nil {
		return err
	}
	fmt.Println("node aliases", list.Size())

	satelliteDB, err := r.WithDatabase.GetSatelliteDB(ctx, log)

	defer func() {
		_ = satelliteDB.Close()
	}()

	cfg := rangedloop.Config{
		Parallelism:        r.Parallelism,
		BatchSize:          2500,
		AsOfSystemInterval: -10 * time.Second,
	}

	var observers []rangedloop.Observer

	observers = append(observers, NewCount())
	observers = append(observers, metrics.NewObserver())
	if !r.NodeID.IsZero() {
		observers = append(observers, NewPieceList(r.NodeID, r.Output))
	}
	var provider rangedloop.RangeSplitter

	switch r.ScanType {
	case "test", "placement", "single":
		provider = NewFullScan(metabaseDB, r.ScanType)
	case "full":
		provider = rangedloop.NewMetabaseRangeSplitter(log, metabaseDB, cfg)
	case "avro":
		segmentPattern := fmt.Sprintf("%s*/%s/%s-*/segments.avro-*", r.BackupDay, r.BackupDatabase, r.Instance)
		segmentsAvroIterator := rangedloop.NewAvroGCSIterator(r.BackupBucket, segmentPattern)
		metainfoPattern := fmt.Sprintf("%s*/%s/%s-*/node_aliases.avro-*", r.BackupDay, "metainfo", r.Instance)
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
