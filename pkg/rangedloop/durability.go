package rangedloop

import (
	"context"
	"fmt"
	"github.com/zeebo/errs"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase/rangedloop"
	"time"
)

// HealthStat collects the availability conditions for one group.
type HealthStat struct {
	min int
}

// Update updates the stat with one measurement: number of pieces which are available even without the nodes of the selected group.
func (h *HealthStat) Update(num int) {
	if num < h.min || h.min == -1 {
		h.min = num
	}
}

// Merge can merge two stat to one, without loosing information.
func (h *HealthStat) Merge(stat *HealthStat) {
	if stat.min < h.min {
		h.min = stat.min
	}
}

type NodeGetter func(id storj.NodeID) (*FullSelectedNode, error)

type GroupClassifier func(node *FullSelectedNode) string

// Durability  is a calculator which checks the availability of pieces without certain nodes.
type Durability struct {
	nodeGetter NodeGetter
	healthStat map[string]*HealthStat
	groups     []GroupClassifier
}

func NewDurability(nodes []*FullSelectedNode, groups []GroupClassifier) *Durability {
	return &Durability{
		nodeGetter: SelectNodeFromListUsingMap(nodes),
		healthStat: make(map[string]*HealthStat),
		groups:     groups,
	}
}

// Start implements rangedloop.Observer.
func (c *Durability) Start(ctx context.Context, time time.Time) error {
	return nil
}

// Fork implements rangedloop.Observer.
func (c *Durability) Fork(ctx context.Context) (rangedloop.Partial, error) {
	return &DurabilityFork{
		nodeGetter: c.nodeGetter,
		groups:     c.groups,
		healthStat: make(map[string]*HealthStat),
	}, nil
}

// Join implements rangedloop.Observer.
func (c *Durability) Join(ctx context.Context, partial rangedloop.Partial) (err error) {
	defer mon.Task()(&ctx)(&err)
	fork := partial.(*DurabilityFork)
	for name, stat := range fork.healthStat {
		existing, found := c.healthStat[name]
		if !found {
			c.healthStat[name] = stat
		} else {
			existing.Merge(stat)
		}

	}
	return nil
}

// Finish implements rangedloop.Observer.
func (c *Durability) Finish(ctx context.Context) error {
	for name, stat := range c.healthStat {
		fmt.Println(stat.min, name)
	}
	return nil
}

// DurabilityFork is the durability calculator for each segment range.
type DurabilityFork struct {
	nodeGetter NodeGetter
	healthStat map[string]*HealthStat
	groups     []GroupClassifier
}

// Process implements rangedloop.Partial.
func (c *DurabilityFork) Process(ctx context.Context, segments []rangedloop.Segment) (err error) {
	defer mon.Task()(&ctx)(&err)
	for _, segment := range segments {
		controlledByGroup := map[string]int{}
		healthyPieceCount := 0
		for _, piece := range segment.Pieces {
			selectedNode, err := c.nodeGetter(piece.StorageNode)
			if err != nil {
				// DQ or suspended, ignore
				continue
			}
			healthyPieceCount++
			for _, groupClassify := range c.groups {
				groupName := groupClassify(selectedNode)
				controlledByGroup[groupName]++
			}
		}
		for name, count := range controlledByGroup {
			existing, found := c.healthStat[name]
			if !found {
				existing = &HealthStat{
					min: -1,
				}
				c.healthStat[name] = existing
			}
			existing.Update(healthyPieceCount - count)
		}
	}
	return nil
}

var _ rangedloop.Observer = &Durability{}
var _ rangedloop.Partial = &DurabilityFork{}

func SelectNodeFromList(storageNodes []*FullSelectedNode) NodeGetter {
	return func(id storj.NodeID) (*FullSelectedNode, error) {
		for _, node := range storageNodes {
			if node.ID == id {
				return node, nil
			}
		}
		return nil, errs.New("no such node")
	}
}

func SelectNodeFromListUsingMap(storageNodes []*FullSelectedNode) NodeGetter {
	nodeByID := make(map[storj.NodeID]*FullSelectedNode)
	for _, node := range storageNodes {
		nodeByID[node.ID] = node
	}
	return func(id storj.NodeID) (*FullSelectedNode, error) {
		if node, ok := nodeByID[id]; ok {
			return node, nil
		}
		return nil, errs.New("no such node")
	}
}
