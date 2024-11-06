package hashstore

import (
	"storj.io/common/storj"
)

type Compact struct {
	Dir         string
	SatelliteID storj.NodeID
}

func (i *Compact) Run() error {
	return nil
}
