package segment

import (
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
	File   string        `arg:""`
	NodeID *storj.NodeID `help:"opional node ID to filter segments if they are not part of the segment today"`
	db.WithDatabase
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

	input, err := os.Open(s.File)
	if err != nil {
		return errors.WithStack(err)
	}
	defer input.Close()
	cr := csv.NewReader(input)
	for {
		line, err := cr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return errors.WithStack(err)
		}
		seg := line[0]
		if !strings.Contains(seg, "/") {
			seg += "/" + line[1]
		}
		su, sp, err := util.ParseSegmentPosition(line[0])
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
			_, _ = fmt.Fprintf(os.Stderr, "Node is no part of piece list any more: %s/%d\n", segment.StreamID, segment.Position.Encode())
			continue
		}
		repaired := ""
		if segment.RepairedAt != nil {
			repaired = segment.RepairedAt.Format(time.RFC3339)
		}
		fmt.Println(segment.StreamID, segment.Position.Encode(), segment.Placement, segment.CreatedAt.Format(time.RFC3339), repaired)
	}
}

func hasPiece(segment metabase.Segment, id *storj.NodeID) bool {
	for _, piece := range segment.Pieces {
		if piece.StorageNode == *id {
			return true
		}
	}
	return false
}
