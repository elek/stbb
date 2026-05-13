package store

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/storj/storagenode/pieces"
)

type Size struct {
	WithStore
	Cache string
}

func (s Size) Run() error {
	log, err := zap.NewProduction()

	if err != nil {
		return errors.WithStack(err)
	}

	store, err := s.CreateStore(log)
	if err != nil {
		return errors.WithStack(err)
	}
	fw := pieces.NewFileWalker(log, store, nil, nil, nil)

	p := util.Progress{}
	var satPiecesTotal, satPiecesContentSize int64
	start := time.Now()
	err = fw.WalkSatellitePieces(context.Background(), s.Satellite, nil, func(access pieces.StoredPieceAccess) error {
		pieceTotal, pieceContentSize, err := access.Size(context.Background())
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		satPiecesTotal += pieceTotal
		satPiecesContentSize += pieceContentSize
		p.Increment()
		return nil
	})

	fmt.Println(p.Counter(), satPiecesTotal, satPiecesContentSize, time.Since(start))
	return nil
}
