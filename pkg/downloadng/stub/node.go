package stub

import (
	"fmt"
	"storj.io/common/identity"
	"storj.io/common/identity/testidentity"
	"storj.io/common/storj"
)

type stubNodes []*nodeStub

func NewStubNodes(size int) stubNodes {
	result := stubNodes{}
	for i := 0; i < size; i++ {
		result = append(result, NewNodeStub(i))
	}
	return result
}

func (n stubNodes) GetByAddress(address string) (*nodeStub, error) {
	for _, node := range n {
		if node.Address == address {
			return node, nil
		}
	}
	return nil, fmt.Errorf("no such node %s", address)
}

func (n stubNodes) GetByID(u storj.NodeID) (*nodeStub, error) {
	for _, node := range n {
		if node.Identity.ID == u {
			return node, nil
		}
	}
	return nil, fmt.Errorf("no such node %s", u)
}

type nodeStub struct {
	Address  string
	Identity *identity.FullIdentity
	Index    int
}

func NewNodeStub(index int) *nodeStub {
	otherIdentity, err := testidentity.PregeneratedIdentity(int(index), storj.LatestIDVersion())
	if err != nil {
		panic(err)
	}
	return &nodeStub{
		Address:  fmt.Sprintf("10.10.10.%d:1234", index),
		Identity: otherIdentity,
		Index:    index,
	}
}
