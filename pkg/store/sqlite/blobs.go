package sqlite

import (
	"context"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/zeebo/errs"
	"os"
	"storj.io/common/storj"
	"storj.io/storj/storage"
	"time"
)

type BlobStore struct {
	db     *sql.DB
	insert *sql.Stmt
}

func NewBlobStore(f string, synced bool) (*BlobStore, error) {
	_ = os.Remove(f)
	newDb := false
	if _, err := os.Stat(f); os.IsNotExist(err) {
		newDb = true
	}
	dsn := "file:./" + f + "?_journal=WAL"
	if synced {
		dsn += "&_sync=normal"
	} else {
		dsn += "&_sync=off"
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	if newDb {
		_, err = db.Exec("CREATE TABLE blobs (namespace TEXT, key TEXT, content BLOB)")
		if err != nil {
			return nil, errs.Wrap(err)
		}
	}

	insertQuery, err := db.Prepare("INSERT INTO blobs (namespace,key,content) VALUES(?,?,?)")
	if err != nil {
		return nil, errs.Wrap(err)
	}

	return &BlobStore{
		db:     db,
		insert: insertQuery,
	}, nil
}

func (b *BlobStore) Create(ctx context.Context, ref storage.BlobRef, size int64) (storage.BlobWriter, error) {
	return NewWriter(b.insert, ref), nil
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
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) Trash(ctx context.Context, ref storage.BlobRef) error {
	//TODO implement me
	panic("implement me")
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
	//TODO implement me
	panic("implement me")
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
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) CreateVerificationFile(ctx context.Context, id storj.NodeID) error {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) VerifyStorageDir(ctx context.Context, id storj.NodeID) error {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) Close() error {
	return b.db.Close()
}

var _ storage.Blobs = &BlobStore{}
