package bloom

import (
	"archive/zip"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"io"
	"os"
	"storj.io/common/storj"
	"storj.io/uplink"
	"strings"
)

type Find struct {
	NodeID []storj.NodeID `arg:""`
	Bucket string
	Prefix string
}

func (f Find) Run() error {
	accessString := os.Getenv("UPLINK_ACCESS")
	access, err := uplink.ParseAccess(accessString)
	if err != nil {
		return errors.WithStack(err)
	}

	ctx := context.Background()
	project, err := uplink.OpenProject(ctx, access)
	if err != nil {
		return errors.WithStack(err)
	}
	defer project.Close()

	iterator := project.ListObjects(ctx, f.Bucket, &uplink.ListObjectsOptions{
		Prefix:    f.Prefix + "/",
		System:    true,
		Recursive: true,
	})

	i := 1
	for iterator.Next() {
		object := iterator.Item()
		fmt.Println(i, "checking", object.Key)
		i++
		if !strings.HasSuffix(object.Key, ".zip") {
			continue
		}

		reader, err := zip.NewReader(&ChunkReader{
			ctx:     ctx,
			bucket:  f.Bucket,
			key:     object.Key,
			project: project,
		}, object.System.ContentLength)
		if err != nil {
			return errors.WithStack(err)
		}

		found := 0
		for _, file := range reader.File {
			for _, nodeID := range f.NodeID {
				if file.Name == nodeID.String() {
					fmt.Println("FOUND", file.Name, object.Key, nodeID)
					found++
					if found == len(f.NodeID) {
						return nil
					}
				}
			}
		}
	}
	return nil
}

type ChunkReader struct {
	io.ReaderAt

	ctx     context.Context
	key     string
	bucket  string
	project *uplink.Project
}

func (t *ChunkReader) ReadAt(p []byte, off int64) (n int, err error) {
	download, err := t.project.DownloadObject(t.ctx, t.bucket, t.key, &uplink.DownloadOptions{
		Offset: off,
		Length: int64(len(p)),
	})
	if err != nil {
		return -1, err
	}
	defer func() {
		err = errs.Combine(err, download.Close())
	}()

	return io.ReadFull(download, p)
}
