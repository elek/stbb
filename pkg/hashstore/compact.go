package hashstore

import (
	"context"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/storj/storagenode/hashstore"
	"time"
)

type Compact struct {
	LogDir  string `help:"directory of the store" `
	MetaDir string `help:"directory of the hashtable files" `
}

func (i *Compact) Run() error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	ctx := context.Background()

	store, err := hashstore.NewStore(ctx, i.LogDir, i.MetaDir, log)
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
