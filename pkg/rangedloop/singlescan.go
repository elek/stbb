package rangedloop

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"storj.io/common/uuid"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/metabase/rangedloop"
)

type FullScan struct {
	sql *SQLProvider
}

func NewFullScan(db *metabase.DB, scanType string) *FullScan {
	return &FullScan{
		sql: &SQLProvider{
			conn:     db,
			scanType: scanType,
		},
	}
}
func (f *FullScan) CreateRanges(_ context.Context, nRanges int, batchSize int) ([]rangedloop.SegmentProvider, error) {
	if nRanges != 1 {
		return nil, errors.New("Only one segment is allowed")
	}
	return []rangedloop.SegmentProvider{
		f.sql,
	}, nil

}

var _ rangedloop.RangeSplitter = &FullScan{}

type SQLProvider struct {
	conn     *metabase.DB
	scanType string
}

func (s *SQLProvider) Range() rangedloop.UUIDRange {
	end := uuid.Max()
	return rangedloop.UUIDRange{
		Start: new(uuid.UUID),
		End:   &end,
	}
}

func (s *SQLProvider) Iterate(ctx context.Context, fn func([]rangedloop.Segment) error) error {
	ix := 0
	err := s.conn.IterateLoopSegments(ctx, metabase.IterateLoopSegments{
		BatchSize: 10,
	}, func(ctx context.Context, iterator metabase.LoopSegmentsIterator) error {
		var segments []rangedloop.Segment
		var entry metabase.LoopSegmentEntry
		for iterator.Next(ctx, &entry) {

			if entry.Inline() {
				continue
			}
			ix++
			fmt.Println(entry.StreamID)
			segments = append(segments, rangedloop.Segment(entry))
			if ix > 8 {
				break
			}
		}
		err := fn(segments)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

var _ rangedloop.SegmentProvider = &SQLProvider{}
