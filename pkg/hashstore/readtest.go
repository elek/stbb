package hashstore

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"storj.io/storj/storagenode/hashstore"
)

type ReadTest struct {
	WithHashstore
}

func (r *ReadTest) Run() error {
	ctx := context.Background()

	metaFile, logDir := r.GetPath()

	f, err := os.Open(metaFile)
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()

	hashtbl, _, err := hashstore.OpenTable(ctx, f, hashstore.CreateDefaultConfig(0, false))
	if err != nil {
		return errors.WithStack(err)
	}

	// build map of log ID -> file path
	logFiles, err := findLogFiles(logDir)
	if err != nil {
		return errors.WithStack(err)
	}

	// cache open file handles
	var mu sync.Mutex
	openFiles := make(map[uint64]*os.File)
	getFile := func(logID uint64) (*os.File, error) {
		mu.Lock()
		defer mu.Unlock()
		if fh, ok := openFiles[logID]; ok {
			return fh, nil
		}
		path, ok := logFiles[logID]
		if !ok {
			return nil, fmt.Errorf("log file not found for id %d", logID)
		}
		fh, err := os.Open(path)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		openFiles[logID] = fh
		return fh, nil
	}
	defer func() {
		for _, fh := range openFiles {
			_ = fh.Close()
		}
	}()

	today := hashstore.TimeToDateDown(time.Now())

	var total, success, readErr, missingLog, skippedToday int64
	start := time.Now()

	err = hashtbl.Range(ctx, func(_ context.Context, rec hashstore.Record) (bool, error) {
		total++

		if rec.Created >= today {
			skippedToday++
			return true, nil
		}

		fh, err := getFile(rec.Log)
		if err != nil {
			missingLog++
			fmt.Printf("MISSING LOG: key=%s log=%d err=%v\n", hex.EncodeToString(rec.Key[:]), rec.Log, err)
			return true, nil
		}

		var buf [1]byte
		_, err = fh.ReadAt(buf[:], int64(rec.Offset))
		if err != nil {
			readErr++
			fmt.Printf("READ ERROR: key=%s log=%d offset=%d length=%d err=%v\n", hex.EncodeToString(rec.Key[:]), rec.Log, rec.Offset, rec.Length, err)
			return true, nil
		}

		success++

		if total%1000 == 0 {
			elapsed := time.Since(start)
			fmt.Printf("progress: %d records, %d success, %d read errors, %d missing logs, %d skipped today (%.0f rps)\n", total, success, readErr, missingLog, skippedToday, float64(total)/elapsed.Seconds())
		}

		return true, nil
	})
	if err != nil {
		return errors.WithStack(err)
	}

	elapsed := time.Since(start)
	fmt.Printf("\nDone: %d total, %d success, %d read errors, %d missing logs, %d skipped today (%.1fs, %.0f rps)\n",
		total, success, readErr, missingLog, skippedToday, elapsed.Seconds(), float64(total)/elapsed.Seconds())

	return nil
}

func findLogFiles(dir string) (map[uint64]string, error) {
	files := make(map[uint64]string)
	if dir == "" {
		return files, nil
	}
	return files, filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		name := filepath.Base(path)
		if (len(name) != 20 && len(name) != 29) || name[0:4] != "log-" {
			return nil
		}
		logID, err := strconv.ParseUint(name[4:20], 16, 64)
		if err != nil {
			return nil
		}
		files[logID] = path
		return nil
	})
}
