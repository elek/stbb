package audit

import (
	"encoding/hex"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs"
	"storj.io/common/pb"
	"time"
)

func init() {
	{
		cmd := cobra.Command{
			Use: "decode",
		}
		cmd.RunE = func(cmd *cobra.Command, args []string) error {
			return decode(args[0])
		}
		AuditCmd.AddCommand(&cmd)
	}
}

func decode(str string) error {
	bytes, err := hex.DecodeString(str)
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
