package taskqueue

import (
	"github.com/elek/stbb/pkg/db"
	"storj.io/common/storj"
)

// Dst prints a histogram of destination nodes in the task queue.
type Dst struct {
	db.WithDatabase
	Address    string   `help:"Redis URL for task queue" default:"redis://localhost:6379"`
	Stream     string   `arg:"" help:"Redis stream name" default:"balancer"`
	Attributes []string `help:"Node attributes to display" default:"tag:server_group,tag:host,tag:service"`
}

func (d *Dst) Run() error {
	return nodeHistogram(histogramConfig{
		WithDatabase: d.WithDatabase,
		Address:      d.Address,
		Stream:       d.Stream,
		Attributes:   d.Attributes,
		nodeSelector: func(src, dst storj.NodeID) storj.NodeID { return dst },
	})
}
