package segment

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"storj.io/infectious"
	"storj.io/uplink/private/eestream"
)

type ECDecode struct {
	StreamID    string `arg:""`
	Incremental bool   `help:"if true, segment will be decoded only if the file doesn't exist'"`
	Correct     bool
	K           int `help:"The k number of RS code, default is 29" default:"29"`
}

func (s *ECDecode) Run() error {
	su, sp, err := util.ParseSegmentPosition(s.StreamID)
	if err != nil {
		return err
	}

	outputFile := fmt.Sprintf("segment_%s_%d.bin", su, sp.Encode())
	if s.Incremental {
		if stat, err := os.Stat(outputFile); err == nil && stat.Size() != 0 {
			fmt.Printf("Output file already exists, skipping decoding: %s/%d\n", su, sp.Encode())
			return nil
		}
	}

	pieces := map[int][]byte{}
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
		pieces[num] = data

	}
	fmt.Printf("%d shares are loaded for %s/%d\n", len(pieces), su, sp.Encode())

	var out *os.File
	if !s.Correct {
		out, err = os.Create(outputFile)
		if err != nil {
			return errors.WithStack(err)
		}
		defer out.Close()
	}

	startOffset := 0
	outb := make([]byte, 0)
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
		for ix, data := range pieces {
			shares = append(shares, infectious.Share{
				Number: ix,
				Data:   data[startOffset:endOffset],
			})
			if ix > maxix {
				maxix = ix
			}
		}
		startOffset += 256
		fec, err := infectious.NewFEC(s.K, maxix+1)
		if err != nil {
			return errors.WithStack(err)
		}

		scheme := eestream.NewRSScheme(fec, 256)

		if s.Correct {
			original := makeCopies(shares)
			err := fec.Correct(shares)
			if err != nil {
				return errors.WithStack(err)
			}

			for _, share := range shares {

				if !bytes.Equal(original[share.Number].Data, share.Data) {
					fmt.Println("Piece", share.Number, "was corrupt at the offset", startOffset)
				}
			}
			continue
		}

		decoded, err := scheme.Decode(outb, shares)
		if err != nil {
			return errors.WithStack(err)
		}
		_, err = out.Write(decoded)
		if err != nil {
			return errors.WithStack(err)
		}

	}
	return nil
}

func makeCopies(originals []infectious.Share) map[int]infectious.Share {
	res := map[int]infectious.Share{}
	for _, original := range originals {
		res[original.Number] = eestream.Share{
			Data:   append([]byte{}, original.Data...),
			Number: original.Number,
		}
	}
	return res
}
