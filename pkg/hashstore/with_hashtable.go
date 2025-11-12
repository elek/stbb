package hashstore

import (
	"context"
	"fmt"
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
	if strings.HasPrefix(w.Path, "@") {
		id, tbl, ok := strings.Cut(strings.TrimPrefix(w.Path, "@"), "/")
		if !ok {
			panic("Use the format @storagenode1234/s0")
		}

		metaPath, err := pickFirstTbl(fmt.Sprintf("/opt/snmeta/%s/hashstore/12EayRS2V1kEsWESU9QMRseFhdxYxKicsiFmxrsLZHeLUtdps3S/%s/meta", id, tbl))
		if err != nil {
			metaPath, err = pickFirstTbl(fmt.Sprintf("/opt/%s/config/storage/hashstore/12EayRS2V1kEsWESU9QMRseFhdxYxKicsiFmxrsLZHeLUtdps3S/%s/meta", id, tbl))
			if err != nil {
				panic("Couldn't find meta directory: " + err.Error())
			}
		}
		path = metaPath
	}

	stat, err := os.Stat(path)
	if err != nil {
		return nil, func() error { return nil }, errors.New("could not stat hashtable path: " + path + " " + err.Error())
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
		tbl, _, err := hashstore.OpenTable(ctx, o, hashstore.CreateDefaultConfig(0, false))
		return tbl, func() error {
			_ = tbl.Close
			return o.Close()
		}, err
	}
}
