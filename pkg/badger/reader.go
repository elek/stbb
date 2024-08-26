package badger

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"io"
	"storj.io/storj/storagenode/blobstore"
	"storj.io/storj/storagenode/blobstore/filestore"
)

type reader struct {
	offset int
	length int
	buffer []byte
}

var _ blobstore.BlobReader = &reader{}

func NewReader(db *badger.DB, ref blobstore.BlobRef) (blobstore.BlobReader, error) {
	r := reader{}
	r.buffer = make([]byte, 0)
	var found bool
	err := db.View(func(txn *badger.Txn) error {
		pref := keyPrefix(ref)
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchSize:   1,
			PrefetchValues: true,
			Prefix:         pref,
		})
		defer it.Close()

		for it.Seek(pref); it.ValidForPrefix(pref); {
			var err error
			r.buffer, err = it.Item().ValueCopy(r.buffer)
			if err != nil {
				return errors.WithStack(err)
			}
			found = true
			break
		}

		r.length = len(r.buffer)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New("missing blob")
	}
	return &r, nil
}
func (r *reader) Read(p []byte) (n int, err error) {
	if r.offset >= r.length {
		return 0, io.EOF
	}
	n = copy(p, r.buffer[r.offset:])
	r.offset += n
	return
}

func (r *reader) ReadAt(p []byte, off int64) (n int, err error) {
	n = copy(p, r.buffer[off:])
	r.offset += n
	return
}

func (r *reader) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekStart {
		panic("implement me")
	}
	r.offset = int(offset)
	return int64(r.offset), nil
}

func (r *reader) Close() error {
	r.buffer = nil
	return nil
}

func (r *reader) Size() (int64, error) {
	return int64(len(r.buffer)), nil
}

func (r *reader) StorageFormatVersion() blobstore.FormatVersion {
	return filestore.FormatV1
}
