package hashstore

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"storj.io/storj/storagenode/hashstore"
)

type WithHashtable struct {
	Path string `arg:"" help:"the path to the hashtable file (or directory with one hashtbl file)"`
}

func (w WithHashtable) Open(ctx context.Context) (hashstore.Tbl, func() error, error) {
	return w.openPath(ctx, w.Path)
}

func (w WithHashtable) openPath(ctx context.Context, path string) (hashstore.Tbl, func() error, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return nil, func() error { return nil }, errors.WithStack(err)
	}
	if stat.IsDir() {
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, func() error { return nil }, errors.WithStack(err)
		}
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "hashtbl-") {
				return w.openPath(ctx, filepath.Join(path, entry.Name()))
			}
		}
		return nil, nil, errors.New("no hashtbl found in directory")
	} else {
		o, err := os.Open(path)
		if err != nil {
			return nil, func() error { return nil }, errors.WithStack(err)
		}
		tbl, err := hashstore.OpenTable(ctx, o, hashstore.CreateDefaultConfig(0, false))
		return tbl, func() error {
			_ = tbl.Close
			return o.Close()
		}, err
	}
}
