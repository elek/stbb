package segment

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/db"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase"
	"strings"
	"time"
)

type Smoketest struct {
	db.WithDatabase
	util.DialerHelper
	WithStreamIDs

	Offset int
	Keys   string `required:"" help:"the satellite identity directory"`
}

type WithStreamIDs struct {
	StreamID string `arg:""`
}

func (w WithStreamIDs) ForEach(callback func(streamID string) error) error {
	_, err := os.Stat(w.StreamID)
	if err == nil {
		file, err := os.ReadFile(w.StreamID)
		if err != nil {
			return errors.WithStack(err)
		}
		for _, streamID := range strings.Split(string(file), "\n") {
			streamID = strings.TrimSpace(streamID)
			if streamID == "" {
				continue
			}
			err := callback(streamID)
			if err != nil {
				fmt.Println("Execution with streamID", streamID, "failed:", err)
			}
		}
		return nil
	}
	return callback(w.StreamID)
}

func (s *Smoketest) Run() error {

	ctx := context.Background()
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	metabaseDB, err := s.WithDatabase.GetMetabaseDB(ctx, log)
	if err != nil {
		return errors.WithStack(err)
	}
	defer metabaseDB.Close()

	satelliteDB, err := s.WithDatabase.GetSatelliteDB(ctx, log)
	if err != nil {
		return errors.WithStack(err)
	}
	defer satelliteDB.Close()

	nodes, err := satelliteDB.OverlayCache().GetAllParticipatingNodes(ctx, 24*time.Hour, -10*time.Millisecond)
	if err != nil {
		return errors.WithStack(err)
	}
	nodeToAddress := map[storj.NodeID]string{}
	for _, node := range nodes {
		nodeToAddress[node.ID] = node.LastIPPort
	}

	satelliteIdentityCfg := identity.Config{
		CertPath: filepath.Join(s.Keys, "identity.cert"),
		KeyPath:  filepath.Join(s.Keys, "identity.key"),
	}
	ident, err := satelliteIdentityCfg.Load()
	if err != nil {
		return err
	}

	keySigner := util.NewKeySignerFromFullIdentity(ident, pb.PieceAction_GET_REPAIR)

	dialer, err := util.GetDialerForIdentity(ctx, ident, true, false)
	if err != nil {
		return err
	}
	output := make(chan string)
	go func() {
		for {
			select {
			case msg := <-output:
				fmt.Println(msg)
			case <-ctx.Done():
				return
			}
		}
	}()
	return s.ForEach(func(streamID string) error {
		fmt.Println("Processing streamID:", streamID)
		su, sp, err := util.ParseSegmentPosition(streamID)
		if err != nil {
			return err
		}

		segment, err := metabaseDB.GetSegmentByPosition(ctx, metabase.GetSegmentByPosition{
			StreamID: su,
			Position: sp,
		})
		if err != nil {
			return errors.WithStack(err)
		}

		for _, piece := range segment.Pieces {

			go func() {
				pieceID := segment.RootPieceID.Derive(piece.StorageNode, int32(piece.Number))

				lastIpPort, found := nodeToAddress[piece.StorageNode]
				if !found {
					return
				}

				snURL, err := storj.ParseNodeURL(fmt.Sprintf("%s@%s", piece.StorageNode.String(), lastIpPort))
				if err != nil {
					output <- "Couldn't parse node URL: " + err.Error()
				}

				conn, err := dialer.DialNodeURL(ctx, snURL)
				if err != nil {
					return
				}

				client := pb.NewDRPCPiecestoreClient(util.NewTracedConnection(conn))

				_, _, err = util.DownloadPiece(ctx, client, keySigner, util.DownloadRequest{
					PieceID:     pieceID,
					Storagenode: snURL,
					Size:        1,
					SatelliteID: ident.ID,
				}, func(bytes []byte, hash *pb.PieceHash, ol *pb.OrderLimit) {
					if hash == nil {
						fmt.Printf("Piece %s from node %s is empty\n", pieceID, piece.StorageNode)
						return
					}
					if len(hash.Hash) == 0 {
						output <- fmt.Sprintf("zero_piece_hash,%s,%d,%s,%s", streamID, piece.Number, pieceID.String(), piece.StorageNode.String())
					}

				})
				_ = conn.Close()
			}()
		}
		return nil
	})

}
