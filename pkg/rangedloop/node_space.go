package rangedloop

import (
	"context"
	"fmt"
	"time"

	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase/rangedloop"
)

type NodeSpace struct {
	placement     storj.PlacementConstraint
	postExpansion map[string]int
	nodes         map[storj.NodeID]string
	expiration    time.Time
}

type NodeSpaceFork struct {
	placement     storj.PlacementConstraint
	postExpansion map[string]int
	expiration    time.Time
	nodes         map[storj.NodeID]string
}

func NewNodeSpace(placement storj.PlacementConstraint, expiration time.Time) *NodeSpace {
	return &NodeSpace{
		placement: placement,
	}
}

func (p *NodeSpace) Start(ctx context.Context, time time.Time) (err error) {
	return nil
}

func (p *NodeSpace) Fork(ctx context.Context) (rangedloop.Partial, error) {
	res := &NodeSpaceFork{
		placement: p.placement,
	}
	return res, nil

}

func (p *NodeSpace) Join(ctx context.Context, partial rangedloop.Partial) error {
	for k, v := range partial.(*NodeSpaceFork).postExpansion {
		p.postExpansion[k] += v
	}
	return nil
}

func (p *NodeSpace) Finish(ctx context.Context) error {
	fmt.Println("post-expansion")
	for k, v := range p.postExpansion {
		fmt.Println(k, v)
	}
	return nil
}

func (p *NodeSpaceFork) Process(ctx context.Context, segments []rangedloop.Segment) error {
	for _, segment := range segments {
		if segment.Placement != 0 && segment.Placement != p.placement {
			continue
		}
		if segment.Inline() {
			continue
		}
		if !p.expiration.IsZero() && segment.Expired(p.expiration) {
			continue
		}

		for _, piece := range segment.Pieces {
			clazz := p.nodes[piece.StorageNode]
			if segment.Placement != 0 {
				clazz = fmt.Sprintf("%s,%d", clazz, segment.Placement)
			}
			p.postExpansion[clazz] += int(segment.PieceSize()) * len(segment.Pieces)
		}

	}
	return nil
}

func (p *NodeSpaceFork) Close() error {
	return nil
}

var _ rangedloop.Observer = &NodeSpace{}
var _ rangedloop.Partial = &NodeSpaceFork{}
