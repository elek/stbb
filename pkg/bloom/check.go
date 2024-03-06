package bloom

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"storj.io/common/bloomfilter"
	"storj.io/common/storj"
	"strings"
)

type Check struct {
	BloomFilterFile string `default:"bloom.filter"`
	PiecesFile      string `default:"pieces.txt"`
}

func (c Check) Run() error {
	rawFilter, err := os.ReadFile(c.BloomFilterFile)
	if err != nil {
		return errors.WithStack(err)
	}
	filter, err := bloomfilter.NewFromBytes(rawFilter)
	if err != nil {
		return errors.WithStack(err)
	}

	pieces, err := os.ReadFile(c.PiecesFile)
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
			return errors.WithStack(err)
		}
		if filter.Contains(pieceID) {
			matched++
		} else {
			missing++
		}
	}
	fmt.Println("missing", missing)
	fmt.Println("matched", matched)
	return nil
}
