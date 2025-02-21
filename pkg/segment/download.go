package segment

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/satellitedb/dbx"
	"storj.io/storj/shared/dbutil"
	"storj.io/storj/shared/dbutil/pgutil"
)

type Download struct {
	StreamID string `arg:""`
	util.DialerHelper
	Keys string `required:""`
}

func (s *Download) Run() error {
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
	source, err = pgutil.EnsureApplicationName(source, "stbb")
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
	fmt.Println("placement", segment.Placement)
	//fmt.Println("size", segment.EncryptedSize/int32(segment.Redundancy.RequiredShares))

	satelliteIdentityCfg := identity.Config{
		CertPath: filepath.Join(s.Keys, "identity.cert"),
		KeyPath:  filepath.Join(s.Keys, "identity.key"),
	}
	ident, err := satelliteIdentityCfg.Load()
	if err != nil {
		return err
	}

	dialer, err := util.GetDialerForIdentity(ctx, ident, false, false)
	if err != nil {
		return err
	}

	keySigner := util.NewKeySignerFromFullIdentity(ident, pb.PieceAction_GET_REPAIR)

	outDir := fmt.Sprintf("segment_%s_%d", su, sp.Encode())
	_ = os.MkdirAll(outDir, 0777)
	for _, piece := range segment.Pieces {
		node, err := satelliteDBX.Get_Node_By_Id(ctx, dbx.Node_Id(piece.StorageNode.Bytes()))
		if err != nil {
			return err
		}

		pieceID := segment.RootPieceID.Derive(piece.StorageNode, int32(piece.Number))

		outFile := filepath.Join(outDir, fmt.Sprintf("%d_%s", piece.Number, pieceID))
		if _, err := os.Stat(outFile); err == nil {
			continue
		}

		snURL, err := storj.ParseNodeURL(fmt.Sprintf("%s@%s", piece.StorageNode.String(), safeStr(node.LastIpPort)))
		if err != nil {
			return err
		}

		conn, err := dialer.DialNodeURL(ctx, snURL)
		if err != nil {
			fmt.Println(err)
			continue
		}

		client := pb.NewDRPCPiecestoreClient(util.NewTracedConnection(conn))

		_, _, err = util.DownloadPiece(ctx, client, keySigner, util.DownloadRequest{
			PieceID:     pieceID,
			Storagenode: snURL,
			Size:        int64(segment.EncryptedSize / 29),
			SatelliteID: ident.ID,
		}, func(bytes []byte, hash *pb.PieceHash, ol *pb.OrderLimit) {
			err := os.WriteFile(outFile, bytes, 0644)
			if err != nil {
				fmt.Println(err)
			}
			if hash != nil {
				err = os.WriteFile(outFile+"."+hash.HashAlgorithm.String(), hash.Hash, 0644)
			}
			if err != nil {
				fmt.Println(err)
			}
		})
		_ = conn.Close()
		if err != nil {
			fmt.Println("ERROR", pieceID, "couldn't be downloaded", snURL, err)
			continue
		}
		fmt.Println(pieceID, "is downloaded from", snURL)

	}
	return nil
}
