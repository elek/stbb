package bloom

import (
	"github.com/pkg/errors"
	"os"
	"storj.io/common/bloomfilter"
	"storj.io/common/memory"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/storj/satellite/internalpb"
	"time"
)

type Generate struct {
	NodeID storj.NodeID `arg:""`
	Size   memory.Size  `default:"2000000"`
	Output string       `default:"bloom.filter"`
}

func (c Generate) Run() error {
	bf := bloomfilter.NewExplicit(1, 10, 2000000)

	err := os.WriteFile(c.Output, bf.Bytes(), 0644)
	if err != nil {
		return errors.WithStack(err)
	}
	filter := make([]byte, 4000000)
	for i := 0; i < len(filter); i++ {
		filter[i] = 1
	}
	info := &internalpb.RetainInfo{
		CreationDate:  time.Now(),
		PieceCount:    1,
		StorageNodeId: c.NodeID,
		Filter:        filter,
	}
	raw, err := pb.Marshal(info)
	if err != nil {
		return errors.WithStack(err)
	}
	err = os.WriteFile(c.Output, raw, 0644)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
