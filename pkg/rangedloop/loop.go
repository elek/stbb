package rangedloop

import (
	"context"
	"fmt"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"os"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/metabase/rangedloop"
	"storj.io/storj/satellite/metrics"
	"storj.io/storj/satellite/satellitedb"
	"time"
)

type RangedLoop struct {
	ScanType  string
	ScanParam int
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

	satelliteDB, err := satellitedb.Open(ctx, log.Named("metabase"), os.Getenv("STBB_DB_SATELLITE"), satellitedb.Options{
		ApplicationName: "stbb",
	})

	nodes, err := GetParticipatingNodes(ctx, satelliteDB.Testing().RawDB())
	if err != nil {
		return errs.New("Error creating satellite connection: %+v", err)
	}

	defer func() {
		_ = satelliteDB.Close()
	}()

	cfg := rangedloop.Config{
		Parallelism:        1,
		BatchSize:          2500,
		AsOfSystemInterval: -10 * time.Second,
	}

	var observers []rangedloop.Observer
	observers = append(observers, NewDurability(nodes, []GroupClassifier{
		func(node *FullSelectedNode) string {
			return "net:" + node.LastNet
		},
		func(node *FullSelectedNode) string {
			return "email:" + node.Email
		},
		func(node *FullSelectedNode) string {
			return "wallet:" + node.Wallet
		},
		func(node *FullSelectedNode) string {
			return "country:" + node.CountryCode.String()
		},
	}))

	observers = append(observers, NewCount())
	observers = append(observers, metrics.NewObserver())
	var provider rangedloop.RangeSplitter

	switch r.ScanType {
	case "test", "placement", "single":
		provider = NewFullScan(metabaseDB, r.ScanType)
	case "full":
		provider = rangedloop.NewMetabaseRangeSplitter(metabaseDB, -10*time.Second, 8)
		cfg.Parallelism = 8
	}
	service := rangedloop.NewService(log.Named("rangedloop"), cfg, provider, observers)
	_, err = service.RunOnce(ctx)
	if err != nil {
		return err
	}
	return nil
}
