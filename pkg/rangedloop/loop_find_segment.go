package rangedloop

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
	"storj.io/common/uuid"
	"storj.io/storj/satellite/metabase/rangedloop"
)

type FindSegment struct {
	WithRangedLoop
	SegmentID uuid.UUID `arg:"" help:"set a SegmentID to find and export segments"`
}

var _ rangedloop.Observer = (*FindSegment)(nil)

func (c *FindSegment) Run() error {
	//ctx := context.Background()
	//log, err := zap.NewDevelopment()
	//if err != nil {
	//	return err
	//}

	return c.RunLoop(func(observers []rangedloop.Observer) []rangedloop.Observer {
		return append(observers, c)
	})
}

func (c *FindSegment) Start(ctx context.Context, time time.Time) error {
	return nil
}

func (c *FindSegment) Fork(ctx context.Context) (rangedloop.Partial, error) {
	return &FindSegmentPartial{
		StreamID: c.SegmentID,
	}, nil
}

func (c *FindSegment) Join(ctx context.Context, partial rangedloop.Partial) error {
	return nil
}

func (c *FindSegment) Finish(ctx context.Context) error {
	return nil
}

type FindSegmentPartial struct {
	StreamID uuid.UUID
}

var _ rangedloop.Partial = (*FindSegmentPartial)(nil)

func (s *FindSegmentPartial) Process(ctx context.Context, segments []rangedloop.Segment) error {
	for _, segment := range segments {
		if segment.StreamID == s.StreamID {
			raw, err := yaml.Marshal(segment)
			if err != nil {
				return errors.WithStack(err)
			}
			err = os.WriteFile(fmt.Sprintf("segment_%s_%d.yaml", segment.StreamID, segment.Position.Encode()), raw, 0644)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}
