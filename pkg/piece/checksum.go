package piece

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"io"
	"storj.io/common/pb"
	"storj.io/storj/storagenode/blobstore"
	"storj.io/storj/storagenode/blobstore/filestore"
)

type Checksum struct {
	Dir string `arg:""`
}

func (c Checksum) Run() error {
	ctx := context.TODO()
	log, _ := zap.NewDevelopment()
	dir, err := filestore.NewDir(log, c.Dir)
	if err != nil {
		return errors.WithStack(err)
	}
	blobs := filestore.New(log, dir, filestore.DefaultConfig)
	ns, err := blobs.ListNamespaces(ctx)
	for _, n := range ns {
		fmt.Println("checking namespace ", hex.EncodeToString(n))
		ix := 0
		err = blobs.WalkNamespace(ctx, n, "", func(info blobstore.BlobInfo) error {
			err := c.checkBlob(ctx, blobs, info)
			ix++
			if err != nil {
				fmt.Println(err.Error())
			}
			return nil
		})
		fmt.Println("checked", ix, "blobs")
	}

	return errors.WithStack(err)
}

func (c Checksum) checkBlob(ctx context.Context, blobs blobstore.Blobs, info blobstore.BlobInfo) (err error) {
	defer mon.Task()(&ctx)(&err)
	reader, err := blobs.Open(ctx, info.BlobRef())
	if err != nil {
		return err
	}
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	if err != nil {
		return errors.WithStack(err)
	}
	size := binary.BigEndian.Uint16(raw[0:2])
	header := &pb.PieceHeader{}
	headerBytes := raw[2 : size+2]
	err = pb.Unmarshal(headerBytes, header)
	if err != nil {
		return errors.WithStack(err)
	}
	hasher := pb.NewHashFromAlgorithm(header.HashAlgorithm)
	_, _ = hasher.Write(raw[512:])
	if !bytes.Equal(hasher.Sum(nil), header.Hash) {
		return errors.New("Hash mismatch: " + hex.EncodeToString(info.BlobRef().Key))
	}
	return nil
}
