package rangedloop

import (
	"context"
	"fmt"
	"time"

	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase/rangedloop"
)

type UsedSpace struct {
	placement     storj.PlacementConstraint
	preExpansion  int
	postExpansion int
	expiration    time.Time
}

type UsedSpaceFork struct {
	placement     storj.PlacementConstraint
	preExpansion  int
	postExpansion int
	expiration    time.Time
}

func NewUsedSpace(placement storj.PlacementConstraint, expiration time.Time) *UsedSpace {
	return &UsedSpace{
		placement:  placement,
		expiration: expiration,
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
	p.preExpansion += partial.(*UsedSpaceFork).preExpansion
	p.postExpansion += partial.(*UsedSpaceFork).postExpansion
	return nil
}

func (p *UsedSpace) Finish(ctx context.Context) error {
	fmt.Printf("Used space for placement %d is: pre-expansion=%d, post-expansion=%d, ratio=%f.2\n", p.placement, p.preExpansion, p.postExpansion, float64(p.postExpansion)/float64(p.preExpansion))
	return nil
}

func (p *UsedSpaceFork) Process(ctx context.Context, segments []rangedloop.Segment) error {
	for _, segment := range segments {
		if segment.Placement != p.placement {
			continue
		}
		if segment.Inline() {
			continue
		}
		if !p.expiration.IsZero() && segment.Expired(p.expiration) {
			continue
		}

		p.preExpansion += int(segment.EncryptedSize)
		p.postExpansion += int(segment.PieceSize()) * len(segment.Pieces)

	}
	return nil
}

func (p *UsedSpaceFork) Close() error {
	return nil
}

var _ rangedloop.Observer = &UsedSpace{}
var _ rangedloop.Partial = &UsedSpaceFork{}
