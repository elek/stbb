package audit

import (
	"encoding/hex"
	"fmt"
	"github.com/zeebo/errs"
	"storj.io/common/pb"
	"time"
)

type Decode struct {
	Value string `arg:"" usage:"hex representation of the audit history"`
}

func (d Decode) Run() error {
	bytes, err := hex.DecodeString(d.Value)
	if err != nil {
		return errs.Wrap(err)
	}
	history := &pb.AuditHistory{}
	err = pb.Unmarshal(bytes, history)
	if err != nil {
		return errs.Wrap(err)
	}
	fmt.Printf("Score %0.01f", history.Score)
	for _, w := range history.Windows {
		fmt.Printf("%s %d %d\n", w.WindowStart.Format(time.RFC3339), w.OnlineCount, w.TotalCount)
	}
	return nil
}
