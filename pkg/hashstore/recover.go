package hashstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"storj.io/storj/storagenode/hashstore"
)

type Recover struct {
	Dir     string `default:"." help:"the directory to recover"`
	MetaDir string `help:"the directory to create the recovered hashtable"`
	Size    int    `default:"26" help:"size of the new hashtable (power of 2)"`
	Kind    int    `default:"1" help:"kind of the hashtable, 0 hashtbl, 1 memtbl"`
}

func (n *Recover) Run() (err error) {
	ctx := context.Background()

	tblDir := n.MetaDir
	if tblDir == "" {
		tblDir = filepath.Join(n.Dir, "meta-recovered")
	}
	_ = os.MkdirAll(tblDir, 0755)
	tblFile, err := os.Create(filepath.Join(tblDir, "hashtbl"))
	if err != nil {
		return errors.WithStack(err)
	}
	defer tblFile.Close()

	tbl, err := hashstore.CreateTable(ctx, tblFile, uint64(n.Size), hashstore.TimeToDateDown(time.Now()), hashstore.TableKind(n.Kind), hashstore.CreateDefaultConfig(0, false))
	if err != nil {
		return errors.WithStack(err)
	}

	return filepath.Walk(n.Dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasPrefix(info.Name(), "log-") {
			return nil
		}
		counter := 0
		start := time.Now()
		err = n.RecoverOne(path, func(record hashstore.Record) error {
			counter++
			ok, err := tbl.Append(ctx, record)
			if err != nil {
				fmt.Println("Couldn't insert record", record.Key.String(), err)
			}
			if !ok {
				return errors.New("Couldn't insert record, hashtable is full")
			}
			return nil
		})
		if err != nil {
			fmt.Println("Couldn't recover data from log file", path, err)
			return nil
		}
		fmt.Println("Recovered", counter, "records from", path, "in", time.Since(start).Seconds(), "seconds (", float64(counter)/time.Since(start).Seconds(), "rps)")
		return nil
	})
}

func (n *Recover) RecoverOne(path string, process func(hashstore.Record) error) error {
	logFile, err := os.Open(path)
	if err != nil {
		return errors.WithStack(err)
	}
	defer logFile.Close()
	readRecord := func(off int64) (rec hashstore.Record, ok bool, err error) {
		var buf [hashstore.RecordSize]byte
		_, err = logFile.ReadAt(buf[:], off)
		if err != nil {
			return hashstore.Record{}, false, errors.WithStack(err)
		}
		ok = rec.ReadFrom(&buf)
		return rec, ok, nil
	}

	off, err := fileSize(logFile)
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Println("Recovering", path, "with size", off)
	off -= hashstore.RecordSize

	for off >= 0 {
		rec, ok, err := readRecord(off)
		if err != nil {
			return errors.WithStack(err)
		}
		if !ok {
			off--
			continue
		}
		err = process(rec)
		if err != nil {
			return errors.WithStack(err)
		}
		off = int64(rec.Offset) - hashstore.RecordSize
	}

	return nil
}

func fileSize(fh *os.File) (int64, error) {
	if fi, err := fh.Stat(); err != nil {
		return 0, errors.WithStack(err)
	} else {
		return fi.Size(), nil
	}
}
