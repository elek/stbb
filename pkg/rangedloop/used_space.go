package rangedloop

import (
	"context"
	"fmt"
	"time"

	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase/rangedloop"
)

type UsedSpace struct {
	placement storj.PlacementConstraint
	size      int
}

type UsedSpaceFork struct {
	placement storj.PlacementConstraint
	size      int
}

func NewUsedSpace(placement storj.PlacementConstraint) *UsedSpace {
	return &UsedSpace{
		placement: placement,
	}
}

func (p *UsedSpace) Start(ctx context.Context, time time.Time) (err error) {
	return nil
}

func (p *UsedSpace) Fork(ctx context.Context) (rangedloop.Partial, error) {
	res := &UsedSpaceFork{
		placement: p.placement,
	}
	return res, nil

}

func (p *UsedSpace) Join(ctx context.Context, partial rangedloop.Partial) error {
	p.size += partial.(*UsedSpaceFork).size
	return nil
}

func (p *UsedSpace) Finish(ctx context.Context) error {
	fmt.Println("Used space for placement", p.placement, "is", p.size, "bytes")
	return nil
}

func (p *UsedSpaceFork) Process(ctx context.Context, segments []rangedloop.Segment) error {
	for _, segment := range segments {
		if segment.Placement != p.placement {
			continue
		}
		p.size += int(segment.EncryptedSize)

	}
	return nil
}

func (p *UsedSpaceFork) Close() error {
	return nil
}

var _ rangedloop.Observer = &UsedSpace{}
var _ rangedloop.Partial = &UsedSpaceFork{}
