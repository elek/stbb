package segment

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"os"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/satellitedb/dbx"
	"storj.io/storj/shared/dbutil"
	"storj.io/storj/shared/dbutil/pgutil"
)

// PieceList reports the status / availability of one single segment.
type PieceList struct {
	StreamID string `arg:""`
	util.DialerHelper
	Keys string
}

func (s *PieceList) Run() error {
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

	driver, source, _, err := dbutil.SplitConnStr(os.Getenv("STBB_DB_SATELLITE"))
	if err != nil {
		return err
	}
	source, err = pgutil.CheckApplicationName(source, "stbb")
	if err != nil {
		return err
	}

	satelliteDBX, err := dbx.Open(driver, source)
	if err != nil {
		return err
	}

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

	fmt.Println("segment", segment.StreamID)
	fmt.Println("placement", segment.Placement)

	for ix, piece := range segment.Pieces {
		node, err := satelliteDBX.Get_Node_By_Id(ctx, dbx.Node_Id(piece.StorageNode.Bytes()))
		if err != nil {
			return err
		}

		pieceID := segment.RootPieceID.Derive(piece.StorageNode, int32(piece.Number))

		snURL, err := storj.ParseNodeURL(fmt.Sprintf("%s@%s", piece.StorageNode.String(), safeStr(node.LastIpPort)))
		if err != nil {
			return err
		}
		fmt.Println(ix, pieceID, snURL, *node.CountryCode, node.Email, node.Wallet)

	}
	return nil
}
