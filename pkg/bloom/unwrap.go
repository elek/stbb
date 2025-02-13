package bloom

import (
	"github.com/pkg/errors"
	"os"
	"storj.io/common/pb"
	"storj.io/storj/satellite/internalpb"
)

type Unwrap struct {
	BloomFilterFile string `arg:""`
}

func (i Unwrap) Run() error {
	rawFilter, err := os.ReadFile(i.BloomFilterFile)
	if err != nil {
		return errors.WithStack(err)
	}
	retainInfo := &internalpb.RetainInfo{}
	err = pb.Unmarshal(rawFilter, retainInfo)
	if err != nil {
		return errors.WithStack(err)
	}
	err = os.WriteFile(i.BloomFilterFile+".unwrapped", retainInfo.Filter, 0644)
	return err
}
