package hashstore

import (
	"context"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/storj/storagenode/hashstore"
)

var DefaultMMapcfg = hashstore.MmapCfg{}

type Compact struct {
	WithHashstore
	AliveFraction          float64 `help:"Fraction of alive data to keep" default:"0.25"`
	DeleteTrashImmediately bool    `help:"Delete trash segments immediately" default:"true"`
	RewriteMultiple        float64 `help:"Limit data size to be rewritten in one cycle" default:"2.0"`
	DefaultKind            int     `help:"Default table kind (0=hashstore,1=memstore)"`
}

func (i *Compact) Run() error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	ctx := context.Background()

	metaFile, logDir := i.GetPath()

	cfg := hashstore.CreateDefaultConfig(0, false)
	cfg.Compaction.AliveFraction = i.AliveFraction
	cfg.Compaction.DeleteTrashImmediately = i.DeleteTrashImmediately
	cfg.Compaction.RewriteMultiple = i.RewriteMultiple
	cfg.TableDefaultKind = hashstore.TableKindCfg{
		Kind: hashstore.TableKind(i.DefaultKind),
	}

	store, err := hashstore.NewStore(ctx, cfg, logDir, filepath.Dir(metaFile), log, nil, nil)
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
