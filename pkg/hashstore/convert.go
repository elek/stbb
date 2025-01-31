package hashstore

import (
	"context"
	"encoding/binary"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"io"
	"os"
	"path/filepath"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/storj/storagenode/blobstore"
	"storj.io/storj/storagenode/blobstore/filestore"
	"storj.io/storj/storagenode/piecestore"
	"strings"
	"time"
)

type Convert struct {
	Dir         string
	Destination string
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
	defer mon.Task()(&ctx)(&err)

	destDir := i.Destination
	if i.Destination == "" {
		destDir = i.Dir
	}
	dest := filepath.Join(destDir, "hashstore")
	_ = os.MkdirAll(dest, 0755)

	op, err := piecestore.NewHashStoreBackend(dest, nil, nil, log)
	if err != nil {
		return errors.WithStack(err)
	}
	defer op.Close()

	buf := make([]byte, 3_000_000)
	header := &pb.PieceHeader{}
	err = store.WalkNamespace(ctx, i.SatelliteID.Bytes(), nil, func(info blobstore.BlobInfo) error {
		defer mon.Task()(&ctx)(&err)
		buf = buf[:0]
		header.Reset()
		err = i.Copy(ctx, store, op, info, buf, header)
		if err != nil {
			log.Warn("Error on copying blob", zap.ByteString("ns", info.BlobRef().Namespace), zap.ByteString("key", info.BlobRef().Key), zap.Error(err))
		}

		return nil
	})
	return err
}

func (i *Convert) Copy(ctx context.Context, store blobstore.Blobs, op *piecestore.HashStoreBackend, info blobstore.BlobInfo, buf []byte, header *pb.PieceHeader) (err error) {
	defer mon.Task()(&ctx)(&err)
	path, err := info.FullPath(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	source, err := ReadFull(path, buf)
	if err != nil {
		return errors.WithStack(err)
	}

	var satelliteID storj.NodeID
	var pieceID storj.PieceID
	copy(satelliteID[:], info.BlobRef().Namespace)
	copy(pieceID[:], info.BlobRef().Key)

	lengthBytes := source[0:2]
	headerSize := binary.BigEndian.Uint16(lengthBytes)

	err = pb.Unmarshal(source[2:2+headerSize], header)
	if err != nil {
		return errors.WithStack(err)
	}
	out, err := op.Writer(ctx, satelliteID, pieceID, -1, header.OrderLimit.PieceExpiration)
	if err != nil {
		return errors.WithStack(err)
	}

	_, err = out.Write(source[512:])
	if err != nil {
		_ = out.Cancel(ctx)
		return errors.WithStack(err)
	}

	err = out.Commit(ctx, header)
	if err != nil {
		if strings.Contains(err.Error(), "collision detected") {
			return nil
		}
		return errors.WithStack(err)
	}
	return nil
}

func ReadFull(path string, data []byte) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return data, errors.WithStack(err)
	}
	defer f.Close()
	for {
		n, err := f.Read(data[len(data):cap(data)])
		data = data[:len(data)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return data, err
		}

		if len(data) >= cap(data) {
			d := append(data[:cap(data)], 0)
			data = d[:len(data)]
		}
	}
}
