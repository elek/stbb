package hashstore

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"io"
	"os"
	"path/filepath"
	"storj.io/common/storj"
	"storj.io/storj/storagenode/blobstore"
	"storj.io/storj/storagenode/blobstore/filestore"
	"storj.io/storj/storagenode/pieces"
	"storj.io/storj/storagenode/piecestore"
	"time"
)

type Convert struct {
	Dir         string
	SatelliteID storj.NodeID
}

func (i *Convert) Run() error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}
	dir, err := filestore.OpenDir(log, i.Dir, time.Now())
	if err != nil {
		return errors.WithStack(err)
	}

	store := filestore.New(log, dir, filestore.DefaultConfig)
	defer store.Close()
	ctx := context.Background()

	dest := filepath.Join(i.Dir, "hashstore")
	_ = os.MkdirAll(dest, 0755)
	op := piecestore.NewHashStoreBackend(dest, nil, nil, log)
	defer op.Close()

	fmt.Println("start walking")
	err = store.WalkNamespace(ctx, i.SatelliteID.Bytes(), nil, func(info blobstore.BlobInfo) error {
		defer mon.Task()(&ctx)(&err)
		blob, err := store.Open(ctx, info.BlobRef())
		if err != nil {
			return errors.WithStack(err)
		}
		defer blob.Close()

		reader, err := pieces.NewReader(blob)
		if err != nil {
			return errors.WithStack(err)
		}
		defer reader.Close()

		var satelliteID storj.NodeID
		var pieceID storj.PieceID
		copy(satelliteID[:], info.BlobRef().Namespace)
		copy(pieceID[:], info.BlobRef().Key)

		header, err := reader.GetPieceHeader()
		if err != nil {
			return errors.WithStack(err)
		}

		out, err := op.Writer(ctx, satelliteID, pieceID, header.HashAlgorithm, header.OrderLimit.PieceExpiration)
		_, err = io.Copy(out, reader)
		if err != nil {
			return errors.WithStack(err)
		}

		err = out.Commit(ctx, header)
		if err != nil {
			return errors.WithStack(err)
		}

		return nil
	})
	return err
}
