package store

import (
	"context"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"math/rand"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/common/testrand"
	"storj.io/storj/storagenode/blobstore"
	"storj.io/storj/storagenode/blobstore/filestore"
	"storj.io/storj/storagenode/pieces"
	"time"
)

type Generate struct {
	Path   string `kong:"arg='',default=''"`
	Repeat int
}

func (g Generate) Run() error {

	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	d, err := filestore.NewDir(log, g.Path)
	if err != nil {
		return errors.WithStack(err)
	}
	f := filestore.New(log, d, filestore.Config{})

	ns := make([]byte, 32)
	ctx := context.Background()
	for i := 0; i < g.Repeat; i++ {
		size := rand.Intn(1500000) + 500000
		id := storj.NewPieceID()
		data := testrand.BytesInt(size)
		out, err := f.Create(ctx, blobstore.BlobRef{
			Namespace: ns,
			Key:       id.Bytes(),
		})
		if err != nil {
			return errors.WithStack(err)
		}

		writer, err := pieces.NewWriter(log, out, f, storj.NodeID{}, pb.PieceHashAlgorithm_SHA256)
		if err != nil {
			return errors.WithStack(err)
		}
		_, err = writer.Write(data)
		if err != nil {
			return errors.WithStack(err)
		}

		hash := pb.NewHashFromAlgorithm(pb.PieceHashAlgorithm_SHA256)
		hash.Write(data)
		err = writer.Commit(ctx, &pb.PieceHeader{
			FormatVersion: pb.PieceHeader_FORMAT_V1,
			HashAlgorithm: pb.PieceHashAlgorithm_SHA256,
			Hash:          hash.Sum(nil),
			CreationTime:  time.Now(),
		})

		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
