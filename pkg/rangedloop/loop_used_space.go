package rangedloop

import (
	"time"

	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase/rangedloop"
)

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
