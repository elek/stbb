package badger

import (
	"context"
	"storj.io/storj/storagenode/blobstore"
	"storj.io/storj/storagenode/blobstore/filestore"
	"time"
)

type BlobInfo struct {
	ref     blobstore.BlobRef
	size    int64
	name    string
	modTime time.Time
}

func (i BlobInfo) BlobRef() blobstore.BlobRef {
	return i.ref
}

func (i BlobInfo) StorageFormatVersion() blobstore.FormatVersion {
	return filestore.FormatV1
}

func (i BlobInfo) FullPath(ctx context.Context) (string, error) {
	return "", nil
}

func (i BlobInfo) Stat(ctx context.Context) (blobstore.FileInfo, error) {
	return FileInfo{
		name:    i.name,
		size:    i.size,
		modTime: i.modTime,
	}, nil
}

var _ blobstore.BlobInfo = BlobInfo{}

type FileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

func (f FileInfo) ModTime() time.Time {
	return f.modTime
}

func (f FileInfo) Name() string {
	return f.name
}

func (f FileInfo) Size() int64 {
	return f.size
}

var _ blobstore.FileInfo = &FileInfo{}
