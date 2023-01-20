package boltstore

import (
	"context"
	"github.com/zeebo/errs"
	"go.etcd.io/bbolt"
	"io"
	"storj.io/storj/storage"
	"storj.io/storj/storage/filestore"
)

type writer struct {
	offset int
	length int
	buffer []byte
	ref    storage.BlobRef
	db     *bbolt.DB
}

func NewWriter(db *bbolt.DB, ref storage.BlobRef) *writer {
	return &writer{
		db:     db,
		ref:    ref,
		buffer: make([]byte, 5000000),
	}
}
func (w *writer) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekStart {
		panic("implement me")
	}
	w.offset = int(offset)
	if w.length < w.offset {
		w.length = w.offset
	}
	return int64(w.offset), nil
}

func (w *writer) Cancel(ctx context.Context) error {
	return nil
}

func (w *writer) Commit(ctx context.Context) error {
	return w.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(w.ref.Namespace)
		var err error
		if bucket == nil {
			bucket, err = tx.CreateBucket(w.ref.Namespace)
			if err != nil {
				return errs.Wrap(err)
			}
		}
		return bucket.Put(w.ref.Key, w.buffer[0:w.length])
	})
}

func (w *writer) Size() (int64, error) {
	return int64(w.length), nil
}

func (w *writer) StorageFormatVersion() storage.FormatVersion {
	return filestore.FormatV1
}

func (w *writer) Write(p []byte) (n int, err error) {
	copy(w.buffer[w.offset:len(p)+w.offset], p)
	w.length += len(p)
	return len(p), nil
}
