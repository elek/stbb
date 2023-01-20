package sqlite

import (
	"context"
	"database/sql"
	"github.com/zeebo/errs"
	"io"
	"storj.io/storj/storage"
	"storj.io/storj/storage/filestore"
)

type writer struct {
	offset int
	length int
	buffer []byte
	ref    storage.BlobRef
	insert *sql.Stmt
}

func NewWriter(insert *sql.Stmt, ref storage.BlobRef) *writer {
	return &writer{
		insert: insert,
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
	_, err := w.insert.ExecContext(ctx, w.ref.Namespace, w.ref.Key, w.buffer[0:w.length])
	return errs.Wrap(err)
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
