package badger

import (
	"bytes"
	"context"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"storj.io/common/storj"
	"storj.io/common/sync2"
	"storj.io/storj/storagenode/blobstore"
	"storj.io/storj/storagenode/blobstore/filestore"
	"time"
)

var verificationFileName = "storage-badger-verification"

type BlobStore struct {
	db         *badger.DB
	namespaces [][]byte
	dir        string
	gcLoop     *sync2.Cycle
}

func (b *BlobStore) TrashWithStorageFormat(ctx context.Context, ref blobstore.BlobRef, formatVer blobstore.FormatVersion, timestamp time.Time) error {
	if formatVer != filestore.FormatV1 {
		return errs.New("Unsupported format")
	}
	return b.Trash(ctx, ref, timestamp)
}

func (b *BlobStore) CheckWritability(ctx context.Context) error {
	return nil
}

func (b *BlobStore) DeleteTrashNamespace(ctx context.Context, namespace []byte) (err error) {
	//TODO: this would help to forget satellite
	return nil
}

func (b *BlobStore) TryRestoreTrashBlob(ctx context.Context, ref blobstore.BlobRef) error {
	// TODO: restore blob
	return nil
}

func (b *BlobStore) DiskInfo(ctx context.Context) (blobstore.DiskInfo, error) {
	// TODO: use real numbers
	return blobstore.DiskInfo{
		TotalSpace:     1000000000000000,
		AvailableSpace: 1000000000000000,
	}, nil
}

var mon = monkit.Package()

var _ blobstore.Blobs = &BlobStore{}

func NewBlobStore(dir string) (*BlobStore, error) {
	options := badger.DefaultOptions(dir)
	options.ValueThreshold = 10
	options.WithValueLogFileSize(10_000_000_000)
	//options.Logger = badgerlogger.ZapLogger{
	//	Log: log,
	//}
	db, err := badger.Open(options)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	namespaces := make([][]byte, 0)
	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		for it.Seek(namespacePrefix); it.ValidForPrefix(namespacePrefix); it.Next() {
			namespaces = append(namespaces, it.Item().Key()[len(namespacePrefix):])
		}
		it.Close()
		return nil
	})
	loop := sync2.NewCycle(10 * time.Minute)
	go func() {
		err = loop.Run(context.Background(), GC(db))
		if err != nil {
			fmt.Println("error", err.Error())
		}
	}()
	return &BlobStore{
		dir:        dir,
		db:         db,
		namespaces: namespaces,
		gcLoop:     loop,
	}, nil
}

func GC(db *badger.DB) func(ctx context.Context) error {
	return func(ctx context.Context) (err error) {
		mon.Task()(&ctx)(&err)
		err = db.RunValueLogGC(0.05)
		if errs.Is(err, badger.ErrNoRewrite) {
			err = nil
		} else {
			mon.Counter("badger_rewrite").Inc(1)
		}
		return err
	}
}

func (b *BlobStore) Create(ctx context.Context, ref blobstore.BlobRef) (blobstore.BlobWriter, error) {
	err := b.ensureNamespace(ref)
	return NewWriter(b.db, ref), err
}

func (b *BlobStore) Open(ctx context.Context, ref blobstore.BlobRef) (blobstore.BlobReader, error) {
	return NewReader(b.db, ref)
}

func (b *BlobStore) OpenWithStorageFormat(ctx context.Context, ref blobstore.BlobRef, formatVer blobstore.FormatVersion) (blobstore.BlobReader, error) {
	if formatVer != filestore.FormatV1 {
		return nil, errs.New("Unsupported format")
	}
	return b.Open(ctx, ref)
}

func (b *BlobStore) Delete(ctx context.Context, ref blobstore.BlobRef) error {
	return b.db.Update(func(txn *badger.Txn) error {
		pref := keyPrefix(ref)
		it := txn.NewIterator(badger.IteratorOptions{Prefix: pref})
		defer it.Close()

		for it.Seek(pref); it.ValidForPrefix(pref); it.Next() {
			key := it.Item().KeyCopy(nil)
			if err := txn.Delete(key); err != nil {
				return fmt.Errorf("error deleting key %s: %w", string(key), err)
			}
		}
		return nil
	})
}

func (b *BlobStore) DeleteWithStorageFormat(ctx context.Context, ref blobstore.BlobRef, formatVer blobstore.FormatVersion) error {
	if formatVer != filestore.FormatV1 {
		return errs.New("Unsupported format")
	}
	return b.Delete(ctx, ref)
}

func (b *BlobStore) DeleteNamespace(ctx context.Context, ref []byte) (err error) {
	ns := append(namespacePrefix, ref...)
	//TODO: remove namespaces from b.namespaces
	return b.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(ns); it.ValidForPrefix(ns); it.Next() {
			err = txn.Delete(it.Item().Key())
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BlobStore) Trash(ctx context.Context, ref blobstore.BlobRef, timestamp time.Time) error {
	return b.db.Update(func(txn *badger.Txn) error {
		pref := keyPrefix(ref)
		it := txn.NewIterator(badger.IteratorOptions{Prefix: pref})
		defer it.Close()

		for it.Seek(pref); it.ValidForPrefix(pref); it.Next() {
			key := it.Item().KeyCopy([]byte{})
			err := it.Item().Value(func(val []byte) error {
				// we replace the prefix blobs with prefix trash
				return txn.SetEntry(badger.NewEntry(append(trashPrefix, key[5:]...), val).WithTTL(7 * 24 * time.Hour))
			})
			if err != nil {
				return errors.WithStack(err)
			}
			if err := txn.Delete(key); err != nil {
				return fmt.Errorf("error deleting key %s: %w", string(key), err)
			}
		}
		return nil
	})
}

func (b *BlobStore) move(txn *badger.Txn, from []byte, to []byte) error {
	item, err := txn.Get(from)
	if err != nil {
		return err
	}
	err = item.Value(func(val []byte) error {
		return txn.Set(to, val)
	})
	if err != nil {
		return err
	}
	return txn.Delete(from)
}

func (b *BlobStore) RestoreTrash(ctx context.Context, namespace []byte) ([][]byte, error) {
	var keys [][]byte
	err := b.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(trashPrefix); it.ValidForPrefix(trashPrefix); it.Next() {
			key := it.Item().Key()
			keys = append(keys, key)
			origKey := append(blobPrefix, key[len(trashPrefix):]...)
			err := b.move(txn, key, origKey)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return keys, err
}

func (b *BlobStore) EmptyTrash(ctx context.Context, namespace []byte, trashedBefore time.Time) (int64, [][]byte, error) {
	var keys [][]byte
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{PrefetchValues: false})
		defer it.Close()
		for it.Seek(trashPrefix); it.ValidForPrefix(trashPrefix); it.Next() {
			key := it.Item().Key()
			keys = append(keys, key)
			err := txn.Delete(key)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return 0, keys, err
}

func (b *BlobStore) Stat(ctx context.Context, ref blobstore.BlobRef) (blobstore.BlobInfo, error) {
	info := BlobInfo{}
	pref := keyPrefix(ref)
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: pref, PrefetchValues: false})
		defer it.Close()

		for it.Seek(pref); it.ValidForPrefix(pref); {

			break

		}
		return nil
	})
	return info, err
}

func (b *BlobStore) StatWithStorageFormat(ctx context.Context, ref blobstore.BlobRef, formatVer blobstore.FormatVersion) (blobstore.BlobInfo, error) {
	if formatVer != filestore.FormatV1 {
		return nil, errs.New("Unsupported format")
	}
	return b.Stat(ctx, ref)
}

func (b *BlobStore) SpaceUsedForTrash(ctx context.Context) (int64, error) {
	s := int64(0)
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{PrefetchValues: false})
		defer it.Close()
		for it.Seek(trashPrefix); it.ValidForPrefix(trashPrefix); it.Next() {
			item := it.Item()
			s += item.ValueSize()
		}
		return nil
	})
	return s, err
}

func (b *BlobStore) SpaceUsedForBlobs(ctx context.Context) (int64, error) {
	s := int64(0)
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{PrefetchValues: false})
		defer it.Close()
		for it.Seek(namespacePrefix); it.ValidForPrefix(namespacePrefix); it.Next() {
			item := it.Item()
			s += item.ValueSize()
		}
		return nil
	})
	return s, err
}

func (b *BlobStore) SpaceUsedForBlobsInNamespace(ctx context.Context, namespace []byte) (int64, error) {
	s := int64(0)
	ns := append(namespacePrefix, namespace...)
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{PrefetchValues: false})
		defer it.Close()
		for it.Seek(ns); it.ValidForPrefix(ns); it.Next() {
			item := it.Item()
			s += item.ValueSize()
		}
		return nil
	})
	return s, err
}

func (b *BlobStore) ListNamespaces(ctx context.Context) ([][]byte, error) {
	return b.namespaces, nil
}

func (b *BlobStore) WalkNamespace(ctx context.Context, namespace []byte, startFromPrefix string, walkFunc func(blobstore.BlobInfo) error) error {
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{PrefetchValues: false})
		defer it.Close()
		for it.Seek(ns(namespace)); it.ValidForPrefix(ns(namespace)); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			blobKey := key[len(ns(namespace)) : len(key)-16]
			t, s := stat(key)
			err := walkFunc(BlobInfo{
				ref: blobstore.BlobRef{
					Namespace: namespace,
					Key:       blobKey,
				},
				name:    string(blobKey),
				size:    int64(s),
				modTime: t,
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func ns(namespace []byte) []byte {
	return append(blobPrefix, namespace...)
}

func (b *BlobStore) CreateVerificationFile(ctx context.Context, id storj.NodeID) error {
	f, err := os.Create(filepath.Join(b.dir, verificationFileName))
	if err != nil {
		return err
	}
	defer func() {
		err = errs.Combine(err, f.Close())
	}()
	_, err = f.Write(id.Bytes())
	return err
}

func (b *BlobStore) VerifyStorageDir(ctx context.Context, id storj.NodeID) error {
	content, err := os.ReadFile(filepath.Join(b.dir, verificationFileName))
	if err != nil {
		return err
	}

	if !bytes.Equal(content, id.Bytes()) {
		verifyID, err := storj.NodeIDFromBytes(content)
		if err != nil {
			return errs.New("content of file is not a valid node ID: %x", content)
		}
		return errs.New("node ID in file (%s) does not match running node's ID (%s)", verifyID, id.String())
	}
	return nil
}

func (b *BlobStore) Close() error {
	b.gcLoop.Close()
	return b.db.Close()
}

func (b *BlobStore) ensureNamespace(ref blobstore.BlobRef) error {
	for _, ns := range b.namespaces {
		if bytesEq(ns, ref.Namespace) {
			return nil
		}
	}
	err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(append(namespacePrefix, ref.Namespace...), []byte{1})
	})
	if err != nil {
		return err
	}
	b.namespaces = append(b.namespaces, ref.Namespace)
	return nil
}

func bytesEq(ns []byte, namespace []byte) bool {
	if len(ns) != len(namespace) {
		return false
	}
	for i := 0; i < len(ns); i++ {
		if ns[i] != namespace[i] {
			return false
		}
	}
	return true
}

func diskInfoFromPath(path string) (info DiskInfo, err error) {
	var stat unix.Statfs_t
	err = unix.Statfs(path, &stat)
	if err != nil {
		return DiskInfo{"", -1}, err
	}

	// the Bsize size depends on the OS and unconvert gives a false-positive
	availableSpace := int64(stat.Bavail) * int64(stat.Bsize) //nolint: unconvert
	filesystemID := fmt.Sprintf("%08x%08x", stat.Fsid.Val[0], stat.Fsid.Val[1])

	return DiskInfo{filesystemID, availableSpace}, nil
}

// DiskInfo contains statistics about this dir.
type DiskInfo struct {
	ID             string
	AvailableSpace int64
}
