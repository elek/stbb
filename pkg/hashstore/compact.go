package hashstore

import (
	"context"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/storj/storagenode/hashstore"
	"time"
)

type Compact struct {
	Dir string `usage:"directory of the store (not the db!)" default:"/tmp/store"`
}

func (i *Compact) Run() error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	ctx := context.Background()

	store, err := hashstore.NewStore(ctx, i.Dir, "", log)
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
