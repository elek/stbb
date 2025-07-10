package hashstore

import (
	"context"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"path/filepath"
	"storj.io/storj/storagenode/hashstore"
	"time"
)

type Compact struct {
	WithHashstore
}

func (i *Compact) Run() error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	ctx := context.Background()

	metaFile, logDir := i.GetPath()

	store, err := hashstore.NewStore(ctx, logDir, filepath.Dir(metaFile), log)
	if err != nil {
		return errors.WithStack(err)
	}
	defer store.Close()

	err = store.Compact(ctx, func(ctx context.Context, key hashstore.Key, created time.Time) bool {
		return false
	}, time.Time{})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
