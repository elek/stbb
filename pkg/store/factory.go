package rpc

import (
	//largefile "github.com/elek/storj-largefile-storage"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"storj.io/storj/storage"
	"storj.io/storj/storage/filestore"
)

func createBlobs(s string) (storage.Blobs, error) {
	var blobs storage.Blobs

	if _, err := os.Stat(filepath.Join(s, "storage-dir-verification")); err == nil {
		dir, err := filestore.NewDir(zap.NewNop(), s)
		if err != nil {
			return blobs, err
		}
		//dir.SkipSync = skipSync
		blobs = filestore.New(zap.NewNop(), dir, filestore.DefaultConfig)
		return blobs, nil
	}
	if _, err := os.Stat(filepath.Join(s, "storage-badger-verification")); err == nil {
		return badger.NewBlobStore(s)
	}
	//if _, err := os.Stat(filepath.Join(s, "storage-largefile-verification")); err == nil {
	//	return largefile.NewBlobStore(os.Getenv("STORJ_LARGEFILE_CONN"), s)
	//}
	return nil, errs.New("Directory %s doesn't contain initialized storage directory", s)

}
