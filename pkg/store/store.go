package store

import (
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"path/filepath"
	"storj.io/common/process"
	"storj.io/common/storj"
	"storj.io/storj/storagenode/blobstore"
	"storj.io/storj/storagenode/blobstore/filestore"
	"storj.io/storj/storagenode/blobstore/statcache"
	"time"
)

type Store struct {
	Generate Generate `cmd:""`
	Size     Size     `cmd:""`
}

type WithStore struct {
	Dir         string       `arg:"" help:"directory to store pieces in"`
	Satellite   storj.NodeID `default:"1PFhx8cVX2gmesYbRooS3Banj3eBKsLgibQorQuhCwWGHg66U6"`
	BadgerCache string
}

func (w WithStore) CreateStore(log *zap.Logger) (blobstore.Blobs, error) {
	piecesDir, err := filestore.OpenDir(log, w.Dir, time.Now())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	blobStore := filestore.New(log, piecesDir, filestore.Config{})
	if w.BadgerCache != "" {
		flog := process.NamedLog(log, "filestatcache")
		cache, err := statcache.NewBadgerCache(flog, filepath.Join(w.BadgerCache, "filestatcache"))
		if err != nil {
			return nil, errs.Wrap(err)
		}
		return statcache.NewCachedStatBlobStore(flog, cache, blobStore), nil
	}
	return blobStore, nil
}
