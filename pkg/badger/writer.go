package badger

import (
	"context"
	"github.com/dgraph-io/badger/v4"
	"github.com/zeebo/errs"
	"io"
	"storj.io/storj/storagenode/blobstore/filestore"
	"time"

	"storj.io/storj/storagenode/blobstore"
)

type writer struct {
	offset int
	length int
	buffer []byte
	ref    blobstore.BlobRef
	db     *badger.DB
}

func (w *writer) ReserveHeader(i int64) error {
	return nil
}

func NewWriter(db *badger.DB, ref blobstore.BlobRef) *writer {
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
	if w.offset > w.length {
		w.length = w.offset
	}
	return int64(w.offset), nil
}

func (w *writer) Cancel(ctx context.Context) error {
	w.buffer = nil
	return nil
}

func (w *writer) Commit(ctx context.Context) error {
	if w.buffer == nil {
		return errs.New("Already committed")
	}
	err := w.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(key(w.ref, time.Now(), w.offset), w.buffer[:w.offset])
		return txn.SetEntry(entry)
	})
	w.buffer = nil
	return err

}

func (w *writer) Size() (int64, error) {
	return int64(w.offset), nil
}

func (w *writer) StorageFormatVersion() blobstore.FormatVersion {
	return filestore.FormatV1
}

func (w *writer) Write(p []byte) (n int, err error) {
	i := copy(w.buffer[w.offset:len(p)+w.offset], p)
	w.offset += i
	if w.offset > w.length {
		w.length = w.offset
	}
	return i, nil
}
