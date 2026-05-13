package hashstore

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"storj.io/storj/storagenode/hashstore"
)

type Perf struct {
	WithHashstore
	Order string `default:"random" enum:"random,storage" help:"read order: random or storage (sequential by log file)"`
}

type pieceEntry struct {
	Log    uint64
	Offset uint64
	Length uint32
}

func (p *Perf) Run() error {
	ctx := context.Background()

	metaFile, logDir := p.GetPath()

	f, err := os.Open(metaFile)
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()

	hashtbl, _, err := hashstore.OpenTable(ctx, f, hashstore.CreateDefaultConfig(0, false))
	if err != nil {
		return errors.WithStack(err)
	}

	// Phase 1: collect all entries.
	fmt.Println("Collecting entries from hashtable...")
	prog := util.Progress{}
	var entries []pieceEntry
	err = hashtbl.Range(ctx, func(_ context.Context, rec hashstore.Record) (bool, error) {
		if rec.Length == 0 {
			return true, nil
		}
		entries = append(entries, pieceEntry{
			Log:    rec.Log,
			Offset: rec.Offset,
			Length: rec.Length,
		})
		prog.Increment()
		return true, nil
	})
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Printf("Collected %d entries.\n", len(entries))

	if len(entries) == 0 {
		fmt.Println("No entries to read.")
		return nil
	}

	// Phase 2: sort entries.
	switch p.Order {
	case "storage":
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].Log != entries[j].Log {
				return entries[i].Log < entries[j].Log
			}
			return entries[i].Offset < entries[j].Offset
		})
		fmt.Println("Sorted by storage order (log, offset).")
	default:
		rand.Shuffle(len(entries), func(i, j int) {
			entries[i], entries[j] = entries[j], entries[i]
		})
		fmt.Println("Shuffled to random order.")
	}

	// Build log file map and file handle cache.
	logFiles, err := findLogFiles(logDir)
	if err != nil {
		return errors.WithStack(err)
	}

	openFiles := make(map[uint64]*os.File)
	getFile := func(logID uint64) (*os.File, error) {
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

	// Phase 3: read pieces and measure.
	fmt.Println("Reading pieces...")
	total := len(entries)
	var success, readErr, missingLog int64
	var bytesRead int64
	buf := make([]byte, 4*1024*1024) // reusable 4MB buffer

	start := time.Now()
	
	for i, e := range entries {
		fh, err := getFile(e.Log)
		if err != nil {
			missingLog++
			continue
		}

		if int(e.Length) > len(buf) {
			buf = make([]byte, e.Length)
		}
		readBuf := buf[:e.Length]
		_, err = fh.ReadAt(readBuf, int64(e.Offset))
		if err != nil {
			readErr++
			continue
		}

		success++
		bytesRead += int64(e.Length)

		if (i+1)%1000 == 0 {
			elapsed := time.Since(start)
			pct := float64(i+1) / float64(total) * 100
			rps := float64(i+1) / elapsed.Seconds()
			mbps := float64(bytesRead) / 1024 / 1024 / elapsed.Seconds()
			fmt.Printf("progress: %.1f%% (%d/%d) %.0f reads/s %.1f MB/s\n", pct, i+1, total, rps, mbps)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("\nDone: %d total, %d success, %d read errors, %d missing logs\n",
		total, success, readErr, missingLog)
	fmt.Printf("Duration: %.1fs\n", elapsed.Seconds())
	fmt.Printf("Throughput: %.0f ops/s, %.1f MB/s\n",
		float64(total)/elapsed.Seconds(),
		float64(bytesRead)/1024/1024/elapsed.Seconds())
	fmt.Printf("Bytes read: %d (%.1f GB)\n", bytesRead, float64(bytesRead)/1024/1024/1024)

	return nil
}
