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

type MultiCheck struct {
	Filters []string `default:""`
	Pieces  string   `default:"" required:"true"`
	Proto   bool     `help:"force protobuf based deserialization" default:"false"`
}

func (c MultiCheck) Run() error {
	var filters []*bloomfilter.Filter
	for _, filter := range c.Filters {
		of, err := OpenFilter(filter, c.Proto)
		if err != nil {
			return errors.WithStack(err)
		}
		filters = append(filters, of)
	}

	historgram := map[string]int{}
	if _, err := os.Stat(c.Pieces); err == nil {
		pieces, err := os.ReadFile(c.Pieces)
		if err != nil {
			return errors.WithStack(err)
		}

		for _, line := range strings.Split(string(pieces), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			pieceID, err := storj.PieceIDFromString(line)
			if err != nil {
				return errors.Wrap(err, "Invalid line: "+line)
			}

			pattern := ""
			for _, filter := range filters {
				if filter.Contains(pieceID) {
					pattern += "1"
				} else {
					pattern += "0"
				}
			}
			if suspiciousPattern(pattern) > 1 {
				//fmt.Println(pieceID, pattern)
			}
			historgram[pattern]++
		}
	}
	for k, v := range historgram {
		if suspiciousPattern(k) > 1 {
			fmt.Println(k, v)
		}
	}
	return nil
}

func suspiciousPattern(pattern string) int {
	life := 0
	for i := 0; i < len(pattern); i++ {
		if i == 0 && pattern[i] == '1' {
			life++
		}
		if i > 0 && pattern[i] == '1' && pattern[i-1] == '0' {
			life++
		}
	}
	return life
}

func OpenFilter(filter string, proto bool) (*bloomfilter.Filter, error) {
	rawFilter, err := os.ReadFile(filter)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if strings.HasSuffix(filter, ".pb") || proto {
		retainInfo := &internalpb.RetainInfo{}
		err = pb.Unmarshal(rawFilter, retainInfo)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		rawFilter = retainInfo.Filter
	}
	return bloomfilter.NewFromBytes(rawFilter)
}
