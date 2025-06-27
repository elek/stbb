package rangedloop

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"io"
	"os"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase/rangedloop"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/repair"
	"time"
)

type Checker struct {
	nodeCache map[storj.NodeID]nodeselection.SelectedNode
	closers   []io.Closer
	threshold int
}

var _ rangedloop.Observer = (*Checker)(nil)

func NewChecker(allNodes []nodeselection.SelectedNode, threshold int) *Checker {
	nodeMap := map[storj.NodeID]nodeselection.SelectedNode{}
	for _, node := range allNodes {
		nodeMap[node.ID] = node
	}
	return &Checker{
		nodeCache: nodeMap,
		threshold: threshold,
	}
}

func (c *Checker) Start(ctx context.Context, time time.Time) error {
	return nil
}

func (c *Checker) Fork(ctx context.Context) (rangedloop.Partial, error) {
	output, err := os.Create(fmt.Sprintf("unhealthy-%d", len(c.closers)))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	c.closers = append(c.closers, output)
	return &CheckerFork{
		nodeCache: c.nodeCache,
		output:    output,
		threshold: c.threshold,
	}, nil
}

func (c *Checker) Join(ctx context.Context, partial rangedloop.Partial) error {
	return nil
}

func (c *Checker) Finish(ctx context.Context) error {
	for _, closer := range c.closers {
		if err := closer.Close(); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

type CheckerFork struct {
	placements nodeselection.PlacementDefinitions
	output     io.Writer
	nodeCache  map[storj.NodeID]nodeselection.SelectedNode

	// reuse those slices to optimize memory usage
	nodeIDs   []storj.NodeID
	nodes     []nodeselection.SelectedNode
	threshold int
}

func (c *CheckerFork) Process(ctx context.Context, segments []rangedloop.Segment) error {
	for _, segment := range segments {

		pieces := segment.Pieces
		if len(pieces) == 0 {
			return nil
		}

		// reuse fork.nodeIDs and fork.nodes slices if large enough
		if cap(c.nodeIDs) < len(pieces) {
			c.nodeIDs = make([]storj.NodeID, len(pieces))
			c.nodes = make([]nodeselection.SelectedNode, len(pieces))
		} else {
			c.nodeIDs = c.nodeIDs[:len(pieces)]
			c.nodes = c.nodes[:len(pieces)]
		}

		if len(c.nodeIDs) != len(c.nodes) {
			return errs.New("nodeIDs length must be equal to selectedNodes: want %d have %d", len(c.nodeIDs), len(c.nodes))
		}

		for i, piece := range pieces {
			c.nodeIDs[i] = piece.StorageNode
		}
		for ix, nodeID := range c.nodeIDs {
			c.nodes[ix] = c.nodeCache[nodeID]
		}
		result := repair.ClassifySegmentPieces(segment.Pieces, c.nodes, nil, false, false, nodeselection.Placement{})
		if result.Healthy.Count()-int(segment.Redundancy.RequiredShares) <= c.threshold {
			_, err := fmt.Fprintf(c.output, "%s,%d,%d,%d,%d,%d\n", segment.StreamID, segment.Position.Encode(), segment.Placement, segment.Redundancy.RequiredShares, result.Healthy.Count(), result.Retrievable.Count())
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

var _ rangedloop.Partial = (*CheckerFork)(nil)
