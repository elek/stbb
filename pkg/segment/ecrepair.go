package segment

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"golang.org/x/exp/slices"
	"storj.io/infectious"
)

type ECRepair struct {
	StreamID string `arg:""`
	K        int    `help:"The k number of RS code, default is 29" default:"29"`
}

func (s *ECRepair) Run() error {
	su, sp, err := util.ParseSegmentPosition(s.StreamID)
	if err != nil {
		return err
	}

	pieces := []infectious.Share{}
	segmentDir := fmt.Sprintf("segment_%s_%d", su, sp.Encode())
	entries, err := os.ReadDir(segmentDir)
	if err != nil {
		return err
	}
	length := -1
	for _, e := range entries {
		if strings.Contains(e.Name(), ".") {
			// checksum
			continue
		}
		data, err := os.ReadFile(filepath.Join(segmentDir, e.Name()))
		if err != nil {
			return err
		}
		parts := strings.Split(e.Name(), "_")
		if len(parts) < 2 {
			continue
		}
		num, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}
		if length > 0 && length != len(data) {
			return errs.New("Piece with wrong size: %s", e.Name())
		}
		if length == -1 {
			length = len(data)
		}
		pieces = append(pieces, infectious.Share{
			Number: num,
			Data:   data,
		})

	}
	slices.SortFunc(pieces, func(a, b infectious.Share) int {
		return a.Number - b.Number
	})
	rand.Shuffle(len(pieces), func(i, j int) {
		pieces[i], pieces[j] = pieces[j], pieces[i]
	})
	fmt.Printf("%d shares are loaded for %s/%d\n", len(pieces), su, sp.Encode())

	for i := 0; i < len(pieces)-31+1; i++ {
		startOffset := 0
		filteredPieces := pieces[i : i+31]
		fmt.Println(i, len(filteredPieces), numbrs(filteredPieces))
		for {
			var shares []infectious.Share
			if startOffset >= length {
				break
			}
			endOffset := startOffset + 256
			if endOffset > length {
				endOffset = length
			}
			maxix := 0
			for _, p := range filteredPieces {
				shares = append(shares, infectious.Share{
					Number: p.Number,
					Data:   p.Data[startOffset:endOffset],
				})
				if p.Number > maxix {
					maxix = p.Number
				}
			}

			fec, err := infectious.NewFEC(s.K, maxix+1)
			if err != nil {
				return errors.WithStack(err)
			}

			original := makeCopies(shares)
			err = fec.Correct(shares)
			if err != nil {
				fmt.Println(err, "failed to repair segment "+fmt.Sprintf("%s/%d at offset %d-%d out of %d bytes", su, sp.Encode(), startOffset, endOffset, length))
				break
			}
			for _, share := range shares {
				if !bytes.Equal(original[share.Number].Data, share.Data) {
					fmt.Println("Piece", share.Number, "was corrupt at the offset", startOffset)
					break
				}
			}
			startOffset += 256
		}
	}
	return nil
}

func numbrs(pieces []infectious.Share) string {
	var v []string
	for _, p := range pieces {
		v = append(v, fmt.Sprintf("%d", p.Number))
	}
	return strings.Join(v, ",")
}
