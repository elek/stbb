package rangedloop

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/storj/satellite/durability"
	"storj.io/storj/satellite/metabase/rangedloop"
)

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
