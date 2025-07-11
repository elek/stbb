package segment

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/elek/stbb/pkg/db"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"io"
	"os"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase"
	"strings"
	"time"
)

type Report struct {
	db.WithDatabase
	File    string        `arg:""`
	NodeID  *storj.NodeID `help:"optional node ID to filter segments if they are not part of the segment today"`
	InPlace bool          `help:"rewrite the input with actualized data"`
}

func (s *Report) Run() error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	ctx := context.TODO()
	metabaseDB, err := s.GetMetabaseDB(ctx, log.Named("metabase"))
	if err != nil {
		return errs.New("Error creating metabase connection: %+v", err)
	}
	defer func() {
		_ = metabaseDB.Close()
	}()

	firstLine, err := readFirstLine(s.File)
	if err != nil {
		return errors.WithStack(err)
	}

	input, err := os.Open(s.File)
	if err != nil {
		return errors.WithStack(err)
	}
	defer input.Close()

	var out io.Writer
	if s.InPlace {
		buffer := bytes.NewBuffer([]byte{})
		out = buffer
		defer func() {
			err = os.WriteFile(s.File, buffer.Bytes(), 0600)
			if err != nil {
				fmt.Println(err)
			}
		}()
	} else {
		out = os.Stdout
	}
	cout := csv.NewWriter(out)
	defer func() {
		cout.Flush()
	}()
	cr := csv.NewReader(input)
	if !strings.Contains(firstLine, ",") {
		cr.Comma = ' '
		cout.Comma = ' '
	}
	for {
		line, err := cr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return errors.WithStack(err)
		}
		seg := line[0]
		if !strings.Contains(seg, "/") {
			seg += "/" + line[1]
		}
		su, sp, err := util.ParseSegmentPosition(seg)
		if err != nil {
			return err
		}

		segment, err := metabaseDB.GetSegmentByPosition(ctx, metabase.GetSegmentByPosition{
			StreamID: su,
			Position: sp,
		})
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%s/%d %v\n", segment.StreamID, segment.Position.Encode(), err.Error())
			continue
		}
		if s.NodeID != nil && !hasPiece(segment, s.NodeID) {
			_, _ = fmt.Fprintf(os.Stderr, "Node is not part of piece list any more: %s/%d\n", segment.StreamID, segment.Position.Encode())
			continue
		}
		if segment.Expired(time.Now()) {
			_, _ = fmt.Fprintf(os.Stderr, "Segment is expired: %s/%d\n", segment.StreamID, segment.Position.Encode())
			continue
		}
		repaired := ""
		if segment.RepairedAt != nil {
			repaired = segment.RepairedAt.Format(time.RFC3339)
		}
		if !s.InPlace {
			fmt.Println(segment.StreamID, segment.Position.Encode(), segment.Placement, segment.CreatedAt.Format(time.RFC3339), repaired)
		}
	}
	return nil
}

func hasPiece(segment metabase.Segment, id *storj.NodeID) bool {
	for _, piece := range segment.Pieces {
		if piece.StorageNode == *id {
			return true
		}
	}
	return false
}

func readFirstLine(filename string) (string, error) {
	if filename == "" {
		return "", errors.New("filename cannot be empty")
	}

	file, err := os.Open(filename)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		return scanner.Text(), nil
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("error reading file: %w", err)
	}

	return "", errors.New("file is empty")
}
