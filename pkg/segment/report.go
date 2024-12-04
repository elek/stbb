package segment

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"os"
	"storj.io/storj/satellite/metabase"
	"time"
)

type Report struct {
	File string `arg:""`
}

func (s *Report) Run() error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	ctx := context.TODO()
	metabaseDB, err := metabase.Open(ctx, log.Named("metabase"), os.Getenv("STBB_DB_METAINFO"), metabase.Config{
		ApplicationName: "stbb",
	})
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
			return errors.WithStack(err)
		}
		su, sp, err := util.ParseSegmentPosition(line[0])
		if err != nil {
			return err
		}

		segment, err := metabaseDB.GetSegmentByPosition(ctx, metabase.GetSegmentByPosition{
			StreamID: su,
			Position: sp,
		})
		repaired := ""
		if segment.RepairedAt != nil {
			repaired = segment.RepairedAt.Format(time.RFC3339)
		}
		fmt.Println(segment.StreamID, segment.Position.Encode(), segment.Placement, segment.CreatedAt.Format(time.RFC3339), repaired)
	}
}
