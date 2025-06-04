package hashstore

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"storj.io/common/storj"
	"storj.io/storj/storagenode/hashstore"
	"strings"
	"time"
)

type LogRead struct {
	Dir   string `default:"." help:"the directory to recover"`
	Piece string `arg:"" help:"the piece to read from the logs"`
}

func (n *LogRead) Run() (err error) {

	var k hashstore.Key

	decoded, err := storj.PieceIDFromString(n.Piece)
	if err != nil {
		return errors.WithStack(err)
	}
	copy(k[:], decoded[:])

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
			if bytes.Equal(k[:], record.Key[:]) {
				fmt.Println("Record has been found:", record)
				return io.EOF
			}
			counter++
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

func (n *LogRead) RecoverOne(path string, process func(hashstore.Record) error) error {
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
