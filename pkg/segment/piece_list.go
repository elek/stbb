package segment

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"os"
	"storj.io/common/storj"
	"storj.io/common/uuid"
	"storj.io/private/dbutil"
	"storj.io/private/dbutil/pgutil"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/satellitedb/dbx"
	"strconv"
	"strings"
)

// PieceList reports the status / availability of one single segment.
type PieceList struct {
	StreamID string `arg:""`
	util.DialerHelper
	Keys string
}

func ParseUUID(id string) (uuid.UUID, error) {
	if id[0] == '#' {
		sid, _ := uuid.New()
		decoded, err := hex.DecodeString(id[1:])
		if err != nil {
			return uuid.UUID{}, err
		}
		copy(sid[:], decoded)
		fmt.Println(sid.String())
		return sid, nil
	}
	if !strings.Contains(id, "-") {
		id = id[0:8] + "-" + id[8:12] + "-" + id[12:16] + "-" + id[16:20] + "-" + id[20:]
		fmt.Println(id)
	}
	return uuid.FromString(id)
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

	sp := metabase.SegmentPosition{}
	parts := strings.Split(s.StreamID, "/")

	if len(parts) > 1 {
		part, err := strconv.Atoi(parts[1])
		if err != nil {
			return err
		}
		sp = metabase.SegmentPositionFromEncoded(uint64(part))
	}
	su, err := ParseUUID(parts[0])
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
