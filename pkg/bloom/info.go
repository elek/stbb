package bloom

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"os"
	"storj.io/common/bloomfilter"
	"storj.io/common/pb"
	"storj.io/storj/satellite/internalpb"
)

type Info struct {
	BloomFilterFile string `arg:""`
	Request         bool   `help:"Read the binary as a RetainRequest instead of RetainInfo"`
}

func (i Info) Run() error {
	rawFilter, err := os.ReadFile(i.BloomFilterFile)
	if err != nil {
		return errors.WithStack(err)
	}

	var filterBytes []byte
	if i.Request {
		retainInfo := &pb.RetainRequest{}
		err = pb.Unmarshal(rawFilter, retainInfo)
		fmt.Println("created", retainInfo.CreationDate)
		fmt.Println("hash verification err", verifyHash(retainInfo))
		filterBytes = retainInfo.Filter
	} else {
		retainInfo := &internalpb.RetainInfo{}
		err = pb.Unmarshal(rawFilter, retainInfo)
		fmt.Println("created", retainInfo.CreationDate)
		fmt.Println("node", retainInfo.StorageNodeId)
		fmt.Println("piece_count", retainInfo.PieceCount)
		filterBytes = retainInfo.Filter
	}

	filter, err := bloomfilter.NewFromBytes(filterBytes)
	if err != nil {
		return errors.WithStack(err)
	}

	fmt.Println("fill_rate", filter.FillRate())
	fmt.Println("filter_size", filter.Size())

	return nil
}

func verifyHash(req *pb.RetainRequest) any {
	if len(req.Hash) == 0 {
		return nil
	}
	hasher := pb.NewHashFromAlgorithm(req.HashAlgorithm)
	_, err := hasher.Write(req.GetFilter())
	if err != nil {
		return errs.Wrap(err)
	}
	if !bytes.Equal(req.Hash, hasher.Sum(nil)) {
		return errs.New("hash mismatch")
	}
	return nil
}
