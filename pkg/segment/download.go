package segment

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/elek/stbb/pkg/db"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase"
)

type Download struct {
	util.DialerHelper
	db.WithDatabase
	StreamID       string `arg:""`
	Keys           string `required:""`
	NodeInfo       bool
	SegmentMetafil string `help:"Path to segment metafile to read segment info from instead of metabase"`
}

func (s *Download) Run() error {
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

	satelliteDB, err := s.GetSatelliteDB(ctx, log.Named("satellite"))
	if err != nil {
		return errs.New("Error creating metabase connection: %+v", err)
	}
	defer func() {
		_ = satelliteDB.Close()
	}()

	var segment metabase.Segment
	if _, err := os.Stat(s.StreamID); err == nil {
		segment, err = ReadSegmentFile(s.StreamID)
		if err != nil {
			return err
		}
	} else {
		su, sp, err := util.ParseSegmentPosition(s.StreamID)
		if err != nil {
			return err
		}

		segment, err = metabaseDB.GetSegmentByPosition(ctx, metabase.GetSegmentByPosition{
			StreamID: su,
			Position: sp,
		})
		if err != nil {
			return err
		}
	}

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

	outDir := fmt.Sprintf("segment_%s_%d", segment.StreamID.String(), segment.Position.Encode())
	_ = os.MkdirAll(outDir, 0777)

	pieces := 0
	for _, piece := range segment.Pieces {
		nd, err := satelliteDB.OverlayCache().Get(ctx, piece.StorageNode)
		if err != nil {
			return errors.WithStack(err)
		}

		pieceID := segment.RootPieceID.Derive(piece.StorageNode, int32(piece.Number))

		outFile := filepath.Join(outDir, fmt.Sprintf("%d_%s", piece.Number, pieceID))
		if _, err := os.Stat(outFile); err == nil {
			pieces++
			continue
		}

		snURL, err := storj.ParseNodeURL(fmt.Sprintf("%s@%s", piece.StorageNode.String(), nd.LastIPPort))
		if err != nil {
			return err
		}

		conn, err := dialer.DialNodeURL(ctx, snURL)
		if err != nil {
			fmt.Println(err)
			continue
		}

		client := pb.NewDRPCPiecestoreClient(util.NewTracedConnection(conn))
		requiredBytesPerShare := int(math.Ceil(float64(segment.EncryptedSize) / float64(segment.Redundancy.RequiredShares)))
		pad := int(segment.Redundancy.ShareSize)
		size := (requiredBytesPerShare + int(segment.Redundancy.ShareSize)) / pad * pad

		_, _, err = util.DownloadPiece(ctx, client, keySigner, util.DownloadRequest{
			PieceID:     pieceID,
			Storagenode: snURL,
			Size:        int64(size),
			SatelliteID: ident.ID,
		}, func(bytes []byte, hash *pb.PieceHash, limit *pb.OrderLimit) {
			err := os.WriteFile(outFile, bytes, 0644)
			if err != nil {
				fmt.Println(err)
			}
			if hash != nil {
				err = os.WriteFile(outFile+"."+hash.HashAlgorithm.String(), hash.Hash, 0644)
			}
			if hash != nil {
				marshalled, err := pb.Marshal(hash)
				if err != nil {
					panic(err)
				}
				err = os.WriteFile(outFile+".hash", marshalled, 0644)
			}
			if limit != nil {
				marshalled, err := pb.Marshal(limit)
				if err != nil {
					panic(err)
				}
				err = os.WriteFile(outFile+".orderlimit", marshalled, 0644)
			}
			if err != nil {
				fmt.Println(err)
			}
		})
		_ = conn.Close()
		if err != nil {
			tagStr := ""
			tags, terr := satelliteDB.OverlayCache().GetNodeTags(ctx, snURL.ID)
			if terr == nil {
				host, err := tags.FindBySignerAndName(snURL.ID, "host")
				if err == nil {
					tagStr += string(host.Value)
				}
				instance, err := tags.FindBySignerAndName(snURL.ID, "service")
				if err == nil {
					tagStr += "/" + string(instance.Value)
				}

			}

			fmt.Println("ERROR", pieceID, "couldn't be downloaded", snURL, tagStr, err)
			continue
		}
		fmt.Println(pieceID, "is downloaded from", snURL)
		pieces++

	}
	fmt.Println("pieces", pieces, "required", segment.Redundancy.RequiredShares, "in placement", segment.Placement)
	return nil
}
