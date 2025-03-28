package bloom

import (
	"fmt"
	"storj.io/common/bloomfilter"
)

type Parameters struct {
}

func (r Parameters) Run() error {
	hashCount, sizeInBytes := bloomfilter.OptimalParameters(100_000_000, 0.1, 0)
	fmt.Println("hash count: ", hashCount)
	fmt.Println("size in bloom filter: ", sizeInBytes)
	return nil
}
