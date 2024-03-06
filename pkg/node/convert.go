package node

import (
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"storj.io/common/storj"
	"strconv"
	"strings"
)

type Convert struct {
	NodeID     storj.NodeID
	Input      string `arg:""`
	Conversion int
}

// convert rootPieceId,index,... file to pieceId,...
func (c Convert) Run() error {
	switch c.Conversion {
	case 1:
		return c.enrichNodes()
	case 2:
		return c.derivePieceID()
	default:
		panic("unknown conversion")

	}
	return nil
}

func (c Convert) enrichNodes() error {
	input, err := os.Open(c.Input)
	if err != nil {
		return errors.WithStack(err)
	}
	icsv := csv.NewReader(input)
	ocsv := csv.NewWriter(os.Stdout)
	defer ocsv.Flush()
	ix := 0
	for {
		rec, err := icsv.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return errors.WithStack(err)
		}
		if ix > 0 {
			raw, err := hex.DecodeString(rec[0])
			if err != nil {
				return errors.WithStack(err)
			}
			nodeID, err := storj.NodeIDFromBytes(raw)
			if err != nil {
				return errors.WithStack(err)
			}
			rec[0] = nodeID.String()
		}
		err = ocsv.Write(rec)
		if err != nil {
			return errors.WithStack(err)
		}
		ix++
	}
}

func (c Convert) derivePieceID() error {
	pieces, err := os.ReadFile(c.Input)
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

		pn, err := strconv.Atoi(parts[1])
		if err != nil {
			return errors.WithStack(err)
		}
		fmt.Println(pieceID.Derive(c.NodeID, int32(pn)).String())
	}
	return nil
}
