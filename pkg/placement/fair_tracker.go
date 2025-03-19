package placement

import (
	"storj.io/common/storj"
	"storj.io/storj/satellite/nodeselection"
)

type FairTracker struct {
	counters map[storj.NodeID]float64
}

func NewFairTracker() *FairTracker {
	return &FairTracker{
		counters: make(map[storj.NodeID]float64),
	}
}

func (f *FairTracker) Get(uplink storj.NodeID) func(node *nodeselection.SelectedNode) float64 {
	return func(node *nodeselection.SelectedNode) float64 {
		return -1 * f.counters[node.ID]
	}
}

func (f *FairTracker) Update(node *nodeselection.SelectedNode) {
	f.counters[node.ID]++
}

func (f *FairTracker) BumpGeneration() {
	for k, v := range f.counters {
		f.counters[k] = v * 0.9
	}
}

var _ nodeselection.ScoreNode = &FairTracker{}
