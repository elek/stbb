package store

import (
	"context"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/storj/storagenode/blobstore"
	"storj.io/storj/storagenode/pieces"
	"sync"
	"time"
)

type Generate struct {
	WithStore
	Repeat int `default:"10000"`
	Size   int `default:"100000"`
	Thread int `default:"1"`
	log    *zap.Logger
}

func (g Generate) Run() error {
	var err error
	g.log, err = zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	store, err := g.CreateStore(g.log)
	if err != nil {
		return errors.WithStack(err)
	}
	p := &util.Progres{}
	defer store.Close()
	var wg sync.WaitGroup
	for i := 0; i < g.Thread; i++ {
		wg.Add(1)
		data := make([]byte, g.Size)
		for i := 0; i < g.Repeat/g.Thread; i++ {
			err := g.generatePiece(store, data)
			if err != nil {
				return errors.WithStack(err)
			}
			p.Increment()
		}
		wg.Done()
	}
	wg.Wait()
	return nil
}

func (g Generate) generatePiece(store blobstore.Blobs, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Hour)
	defer cancel()
	out, err := store.Create(ctx, blobstore.BlobRef{
		Namespace: g.Satellite.Bytes(),
		Key:       storj.NewPieceID().Bytes(),
	})
	if err != nil {
		return errors.WithStack(err)
	}

	writer, err := pieces.NewWriter(g.log, out, store, g.Satellite, pb.PieceHashAlgorithm_SHA256)
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = writer.Write(data)
	if err != nil {
		return errors.WithStack(err)
	}

	//hash := pb.NewHashFromAlgorithm(pb.PieceHashAlgorithm_SHA256)
	//hash.Write(data)
	err = writer.Commit(ctx, &pb.PieceHeader{
		FormatVersion: pb.PieceHeader_FORMAT_V1,
		HashAlgorithm: pb.PieceHashAlgorithm_SHA256,
		Hash:          []byte{},
		CreationTime:  time.Now(),
	})

	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
