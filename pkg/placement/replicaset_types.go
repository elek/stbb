package placement

import "storj.io/storj/satellite/nodeselection"

type Replicaset struct {
	RequiredNodes int
	Nodes         []*nodeselection.SelectedNode
	Invariant     func([]*nodeselection.SelectedNode, *nodeselection.SelectedNode) bool
}

func (r *Replicaset) Offer(node *nodeselection.SelectedNode) bool {
	if r.Invariant(r.Nodes, node) {
		r.Nodes = append(r.Nodes, node)
		return true
	}
	return false
}

func (r *Replicaset) IsFull() bool {
	return len(r.Nodes) == r.RequiredNodes
}

func (r *Replicaset) Len() int {
	return len(r.Nodes)
}

func InitReplicasets(nodes []*nodeselection.SelectedNode, required int, invariant func([]*nodeselection.SelectedNode, *nodeselection.SelectedNode) bool) []*Replicaset {
	result := make([]*Replicaset, 0)
	for _, node := range nodes {
		var accepted bool
		for _, set := range result {
			if set.IsFull() {
				continue
			}
			accepted = set.Offer(node)
			if accepted {
				break
			}
		}
		if !accepted {
			result = append(result, &Replicaset{
				RequiredNodes: required,
				Invariant:     invariant,
				Nodes:         []*nodeselection.SelectedNode{node},
			})
		}
	}
	return result
}

func Unique(attr nodeselection.NodeAttribute) func([]*nodeselection.SelectedNode, *nodeselection.SelectedNode) bool {
	return func(nodes []*nodeselection.SelectedNode, node *nodeselection.SelectedNode) bool {
		for _, n := range nodes {
			if attr(*n) == attr(*node) {
				return false
			}
		}
		return true
	}
}
