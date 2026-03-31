package taskqueue

import (
	"github.com/elek/stbb/pkg/db"
	"storj.io/common/storj"
)

// Src prints a histogram of source nodes in the task queue.
type Src struct {
	db.WithDatabase
	Address    string   `help:"Redis URL for task queue" default:"redis://localhost:6379"`
	Stream     string   `arg:"" help:"Redis stream name" default:"balancer"`
	Attributes []string `help:"Node attributes to display" default:"tag:server_group,tag:host,tag:service"`
}

func (s *Src) Run() error {
	return nodeHistogram(histogramConfig{
		WithDatabase: s.WithDatabase,
		Address:      s.Address,
		Stream:       s.Stream,
		Attributes:   s.Attributes,
		nodeSelector: func(src, dst storj.NodeID) storj.NodeID { return src },
	})
}
