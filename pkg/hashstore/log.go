package hashstore

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"io/fs"
	"os"
	"path/filepath"
	"storj.io/storj/storagenode/hashstore"
	"strconv"
	"time"
)

type Logs struct {
	Hashstore string `arg:""`
	LogDir    string
}

func (l *Logs) Run() error {
	ctx := context.Background()

	o, err := os.Open(l.Hashstore)
	if err != nil {
		return errors.WithStack(err)
	}

	defer o.Close()
	hashtbl, err := hashstore.OpenHashtbl(ctx, o)
	if err != nil {
		return errors.WithStack(err)
	}
	defer hashtbl.Close()

	// collect statistics about the hash table and how live each of the log files are.
	nset := uint64(0)
	nexist := uint64(0)
	used := make(map[uint64]uint64)
	unused := make(map[uint64]uint64)
	rerr := error(nil)
	modifications := false

	var expired func(e hashstore.Expiration) bool
	var restored func(e hashstore.Expiration) bool
	var shouldTrash func(ctx context.Context, key hashstore.Key, created time.Time) bool

	currentSize, err := findFiles(l.LogDir)

	err = hashtbl.Range(ctx, func(_ context.Context, rec hashstore.Record) (bool, error) {
		rerr = func() error {
			if err != nil {
				return errors.WithStack(err)
			}
			nexist++ // bump the number of records that exist for progress reporting.

			unused[rec.Log] += uint64(rec.Length) + hashstore.RecordSize // rSize for the record footer

			// if we're not yet sure we're modifying the hash table, we need to check our callbacks
			// on the record to see if the table would be modified. a record is modified when it is
			// flagged as trash or when it is restored.
			if !modifications {
				if shouldTrash != nil && !rec.Expires.Trash() && shouldTrash(ctx, rec.Key, hashstore.DateToTime(rec.Created)) {
					modifications = true
				}
				if restored != nil && restored(rec.Expires) {
					modifications = true
				}
			}

			// if the record is expired, we will modify the hash table by not including the record.
			if expired != nil && expired(rec.Expires) {
				modifications = true
				return nil
			}

			// the record is included in the future hash table, so account for it in used space.
			nset++
			used[rec.Log] += uint64(rec.Length) + hashstore.RecordSize // rSize for the record footer

			return nil
		}()
		return rerr == nil, rerr
	})
	fmt.Println("modification", modifications)
	for k, v := range used {
		fmt.Println(k, v, unused[k], currentSize[k])
	}

	return nil
}

func findFiles(dir string) (map[uint64]uint64, error) {
	sizes := make(map[uint64]uint64)
	if dir == "" {
		return sizes, nil
	}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}

		name := filepath.Base(path)

		// skip any files that don't look like log files. log file names are either
		//     log-<16 bytes of id>
		//     log-<16 bytes of id>-<8 bytes of ttl>
		// so they always begin with "log-" and are either 20 or 29 bytes long.
		if (len(name) != 20 && len(name) != 29) || name[0:4] != "log-" {
			return nil
		}

		id, err := strconv.ParseUint(name[4:20], 16, 64)
		if err != nil {
			return errors.Errorf("unable to parse name=%q: %v", name, err)
		}

		info, err := d.Info()
		if err != nil {
			return errors.Errorf("unable to get file info for %q: %v", path, err)
		}
		sizes[id] = uint64(info.Size())

		return err
	})
	return sizes, err
}
