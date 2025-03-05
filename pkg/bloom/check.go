package bloom

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"storj.io/common/bloomfilter"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/storj/satellite/internalpb"
	"strings"
)

type Check struct {
	Filter      string `default:"bloom.filter"`
	Pieces      string `default:"" required:"true"`
	Proto       bool   `help:"force protobuf based deserialization" default:"false"`
	ShowMissing int    `help:"Show this number if missing piece IDs (file based processing)" default:"0"`
}

func (c Check) Run() error {

	rawFilter, err := os.ReadFile(c.Filter)
	if err != nil {
		return errors.WithStack(err)
	}
	if strings.HasSuffix(c.Filter, ".pb") || c.Proto {
		retainInfo := &internalpb.RetainInfo{}
		err = pb.Unmarshal(rawFilter, retainInfo)
		if err != nil {
			return errors.WithStack(err)
		}
		rawFilter = retainInfo.Filter
	}
	filter, err := bloomfilter.NewFromBytes(rawFilter)
	if err != nil {
		return errors.WithStack(err)
	}

	if _, err := os.Stat(c.Pieces); err == nil {
		pieces, err := os.ReadFile(c.Pieces)
		if err != nil {
			return errors.WithStack(err)
		}

		var missing, matched int
		for _, line := range strings.Split(string(pieces), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			pieceID, err := storj.PieceIDFromString(line)
			if err != nil {
				return errors.Wrap(err, "Invalid line: "+line)
			}
			if filter.Contains(pieceID) {
				matched++
			} else {
				missing++
				if c.ShowMissing > 0 {
					fmt.Println("missing", pieceID)
					c.ShowMissing--
				}
			}
		}
		fmt.Println("missing", missing)
		fmt.Println("matched", matched)
	} else {
		pieceID, err := storj.PieceIDFromString(c.Pieces)
		if err != nil {
			return errors.WithStack(err)
		}
		if filter.Contains(pieceID) {
			fmt.Println("matched")
		} else {
			fmt.Println("missing")
		}

	}
	return nil
}
