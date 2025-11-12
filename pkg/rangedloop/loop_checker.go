package rangedloop

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/storj/satellite/metabase/rangedloop"
)

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
