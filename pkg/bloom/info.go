package bloom

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"storj.io/common/bloomfilter"
	"storj.io/common/pb"
	"storj.io/storj/satellite/internalpb"
)

type Info struct {
	BloomFilterFile string `arg:""`
}

func (i Info) Run() error {
	rawFilter, err := os.ReadFile(i.BloomFilterFile)
	if err != nil {
		return errors.WithStack(err)
	}
	retainInfo := &internalpb.RetainInfo{}
	err = pb.Unmarshal(rawFilter, retainInfo)
	fmt.Println("created", retainInfo.CreationDate)
	fmt.Println("node", retainInfo.StorageNodeId)
	fmt.Println("piece_count", retainInfo.PieceCount)
	filter, err := bloomfilter.NewFromBytes(retainInfo.Filter)
	if err != nil {
		return errors.WithStack(err)
	}

	fmt.Println("fill_rate", filter.FillRate())
	fmt.Println("filter_size", filter.Size())

	return nil
}
