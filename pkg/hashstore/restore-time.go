package hashstore

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"os"
	"storj.io/common/storj"
	"storj.io/storj/storagenode/retain"
	"time"
)

type RestoreTime struct {
	NewValue    time.Time    `cmd:"arg"`
	Dir         string       `default:"" help:"the directory to recover"`
	SatelliteID storj.NodeID `default:"12EayRS2V1kEsWESU9QMRseFhdxYxKicsiFmxrsLZHeLUtdps3S"`
}

func (r *RestoreTime) Run() error {
	ctx := context.Background()
	if r.Dir == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return errors.WithStack(err)
		}
		r.Dir = cwd
	}
	mgr := retain.NewRestoreTimeManager(r.Dir)
	if r.NewValue.IsZero() {
		restoreTime := mgr.GetRestoreTime(ctx, r.SatelliteID, time.Now().Add(-time.Hour*24*365))
		fmt.Println(restoreTime)
		return nil
	}
	return mgr.TestingSetRestoreTime(ctx, r.SatelliteID, r.NewValue)
}
