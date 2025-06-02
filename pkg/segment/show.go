package segment

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elek/stbb/pkg/db"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"storj.io/storj/satellite/metabase"
)

type Show struct {
	StreamID string `arg:""`
	db.WithDatabase
}

func (s *Show) Run() error {
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

	su, sp, err := util.ParseSegmentPosition(s.StreamID)
	if err != nil {
		return err
	}

	segment, err := metabaseDB.GetSegmentByPosition(ctx, metabase.GetSegmentByPosition{
		StreamID: su,
		Position: sp,
	})
	if err != nil {
		return err
	}
	pieces := segment.Pieces
	segment.Pieces = nil
	raw, err := json.MarshalIndent(segment, "", "   ")
	if err != nil {
		return err
	}
	fmt.Println(string(raw))

	fmt.Println(segment.RootPieceID.String())

	for _, piece := range pieces {
		fmt.Println(piece.Number, piece.StorageNode, segment.RootPieceID.Derive(piece.StorageNode, int32(piece.Number)))
	}

	return nil
}
