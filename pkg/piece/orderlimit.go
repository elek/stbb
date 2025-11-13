package piece

import (
	"os"

	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"storj.io/common/pb"
)

type Orderlimit struct {
	File string `arg:""`
}

func (s *Orderlimit) Run() error {
	raw, err := os.ReadFile(s.File)
	if err != nil {
		return errors.WithStack(err)
	}
	var ol pb.OrderLimit
	err = pb.Unmarshal(raw, &ol)
	if err != nil {
		return errors.WithStack(err)
	}
	util.PrintStruct(ol)
	return nil
}
