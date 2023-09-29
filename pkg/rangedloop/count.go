package rangedloop

import (
	"context"
	"fmt"
	"github.com/spacemonkeygo/monkit/v3"
	"storj.io/storj/satellite/metabase/rangedloop"
	"sync/atomic"
	"time"
)

var mon = monkit.Package()

type Count struct {
	count atomic.Int64
	start time.Time
}

func (c *Count) Join(ctx context.Context, partial rangedloop.Partial) error {
	return nil
}

func (c *Count) Process(ctx context.Context, segments []rangedloop.Segment) error {
	c.count.Add(int64(len(segments)))
	return nil
}

func NewCount() *Count {
	c := Count{}
	mon.Chain(&c)
	return &c
}

func (c *Count) Stats(cb func(key monkit.SeriesKey, field string, val float64)) {
	cb(monkit.NewSeriesKey("rangedloop_live"), "num_segments", float64(c.count.Load()))
}

func (c *Count) Start(ctx context.Context, time time.Time) error {
	c.start = time
	return nil
}

func (c *Count) Fork(ctx context.Context) (rangedloop.Partial, error) {
	return c, nil
}

func (c *Count) Finish(ctx context.Context) error {
	fmt.Println("segment count", c.count.Load(), "under", time.Since(c.start))
	return nil
}

var _ rangedloop.Observer = &Count{}
var _ rangedloop.Partial = &Count{}
var _ monkit.StatSource = &Count{}
