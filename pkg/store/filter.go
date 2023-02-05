package rpc

import "storj.io/common/storj"

type SatelliteFilter struct {
	id *storj.NodeID
}

func NewSatelliteFilter(id string) SatelliteFilter {
	if id == "" {
		return SatelliteFilter{}
	}
	nodeID, err := storj.NodeIDFromString(id)
	if err != nil {
		panic(err)
	}
	return SatelliteFilter{
		id: &nodeID,
	}
}

func (f SatelliteFilter) Match(nodeID []byte) bool {
	if f.id == nil {
		return true
	}
	ns, err := storj.NodeIDFromBytes(nodeID)
	if err != nil {
		panic(err)
	}
	return *f.id == ns
}
