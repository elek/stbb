package hashstore

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"storj.io/storj/storagenode/hashstore"
	"strings"
)

type WithHashtable struct {
	Path string `arg:"" usage:"the path to the hashtable file (or directory with one hashtbl file)"`
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
				fmt.Println("using hashtbl", entry.Name())
				return w.openPath(ctx, filepath.Join(path, entry.Name()))
			}
		}
		return nil, nil, errors.New("no hashtbl found in directory")
	} else {
		o, err := os.Open(path)
		if err != nil {
			return nil, func() error { return nil }, errors.WithStack(err)
		}
		tbl, err := hashstore.OpenTable(ctx, o)
		return tbl, func() error {
			_ = tbl.Close
			return o.Close()
		}, err
	}
}
