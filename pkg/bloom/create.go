package bloom

import (
	"github.com/pkg/errors"
	"os"
	"storj.io/common/bloomfilter"
	"storj.io/common/memory"
	"storj.io/common/storj"
	"strings"
)

type CreateFilter struct {
	PiecesFile        string      `default:"pieces.txt"`
	FalsePositiveRate float64     `default:"0.01"`
	ExpectedElement   int64       `default:"10000000"`
	MaxMemory         memory.Size `default:"2000000"`
	Output            string      `default:"bloom.filter"`
}

func (c CreateFilter) Run() error {
	count, bytes := bloomfilter.OptimalParameters(c.ExpectedElement, c.FalsePositiveRate, c.MaxMemory)
	seed := bloomfilter.GenerateSeed()
	bf := bloomfilter.NewExplicit(seed, count, bytes)

	pieces, err := os.ReadFile(c.PiecesFile)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, line := range strings.Split(string(pieces), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		pieceID, err := storj.PieceIDFromString(parts[0])
		if err != nil {
			return errors.WithStack(err)
		}
		bf.Add(pieceID)
	}
	err = os.WriteFile(c.Output, bf.Bytes(), 0644)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}
