package boltstore

import (
	"context"
	"github.com/zeebo/errs"
	"go.etcd.io/bbolt"
	"storj.io/common/storj"
	"storj.io/storj/storage"
	"time"
)

var globalBucket = []byte("global")

type BlobStore struct {
	db *bbolt.DB
}

func NewBlobStore() (*BlobStore, error) {
	db, err := bbolt.Open("my.db", 0600, nil)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(globalBucket)
		if b == nil {
			_, err = tx.CreateBucket(globalBucket)
			return err
		}
		return nil
	})
	if err != nil {
		return nil, errs.Wrap(err)
	}
	return &BlobStore{
		db: db,
	}, nil
}
func (b *BlobStore) Create(ctx context.Context, ref storage.BlobRef, size int64) (storage.BlobWriter, error) {
	return NewWriter(b.db, ref), nil
}

func (b *BlobStore) Open(ctx context.Context, ref storage.BlobRef) (storage.BlobReader, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) OpenWithStorageFormat(ctx context.Context, ref storage.BlobRef, formatVer storage.FormatVersion) (storage.BlobReader, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) Delete(ctx context.Context, ref storage.BlobRef) error {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) DeleteWithStorageFormat(ctx context.Context, ref storage.BlobRef, formatVer storage.FormatVersion) error {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) DeleteNamespace(ctx context.Context, ref []byte) (err error) {
	return b.db.Update(func(tx *bbolt.Tx) error {
		return tx.DeleteBucket(ref)
	})
}

func (b *BlobStore) Trash(ctx context.Context, ref storage.BlobRef) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(ref.Namespace)
		if bucket == nil {
			return nil
		}
		return bucket.Delete(ref.Key)
	})
}

func (b *BlobStore) RestoreTrash(ctx context.Context, namespace []byte) ([][]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) EmptyTrash(ctx context.Context, namespace []byte, trashedBefore time.Time) (int64, [][]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) Stat(ctx context.Context, ref storage.BlobRef) (storage.BlobInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) StatWithStorageFormat(ctx context.Context, ref storage.BlobRef, formatVer storage.FormatVersion) (storage.BlobInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) FreeSpace(ctx context.Context) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) CheckWritability(ctx context.Context) error {
	return nil
}

func (b *BlobStore) SpaceUsedForTrash(ctx context.Context) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) SpaceUsedForBlobs(ctx context.Context) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) SpaceUsedForBlobsInNamespace(ctx context.Context, namespace []byte) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) ListNamespaces(ctx context.Context) ([][]byte, error) {

	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) WalkNamespace(ctx context.Context, namespace []byte, walkFunc func(storage.BlobInfo) error) error {
	return b.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(namespace).ForEach(func(k, v []byte) error {
			s := storage.BlobInfo{}
			err := walkFunc(s)
			if err != nil {
				return err
			}
		})
	})
}

func (b *BlobStore) CreateVerificationFile(ctx context.Context, id storj.NodeID) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(globalBucket).Put([]byte("verification"), id.Bytes())
	})
}

func (b *BlobStore) VerifyStorageDir(ctx context.Context, id storj.NodeID) error {
	return b.db.View(func(tx *bbolt.Tx) error {
		ver := tx.Bucket(globalBucket).Get([]byte("verification"))
		if len(ver) != len(id.Bytes()) {
			return errs.New("Verification length mismatch")
		}
		for i, b := range ver {
			if b != id.Bytes()[i] {
				return errs.New("node id mismatch")
			}
		}
		return nil
	})
}

func (b *BlobStore) Close() error {
	return b.db.Close()
}

var _ storage.Blobs = &BlobStore{}
