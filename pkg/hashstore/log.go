package hashstore

import (
	"context"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/pkg/errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"storj.io/common/memory"
	"storj.io/storj/storagenode/hashstore"
	"strconv"
	"time"
)

type Logs struct {
	WithHashtable
	Logs string `arg:""`
}

func (l *Logs) Run() error {
	ctx := context.Background()

	hashtbl, close, err := l.WithHashtable.Open(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	defer close()

	// collect statistics about the hash table and how live each of the log files are.
	nset := uint64(0)
	nexist := uint64(0)

	rerr := error(nil)

	var restored func(e hashstore.Expiration) bool

	today := hashstore.TimeToDateDown(time.Now())

	expired := func(e hashstore.Expiration) bool {
		// if the record does not have an expiration, it is not expired.
		if e == 0 {
			return false
		}
		// if it is not currently after the expiration time, it is not expired.
		if today <= e.Time() {
			return false
		}
		// if it has been restored, it is not expired.
		if restored != nil && restored(e) {
			return false
		}
		// otherwise, it is expired.
		return true
	}

	var shouldTrash func(ctx context.Context, key hashstore.Key, created time.Time) bool

	logFiles, err := findFiles(l.Logs)

	err = hashtbl.Range(ctx, func(_ context.Context, rec hashstore.Record) (bool, error) {
		rerr = func() error {
			if err != nil {
				return errors.WithStack(err)
			}
			nexist++ // bump the number of records that exist for progress reporting.

			if expired(rec.Expires) {
				logFiles[rec.Log].Expired += memory.Size(rec.Length)
			} else if rec.Expires.Trash() {
				logFiles[rec.Log].Trash += memory.Size(rec.Length)
			} else if shouldTrash != nil && shouldTrash(ctx, rec.Key, hashstore.DateToTime(rec.Created)) {
				logFiles[rec.Log].Trash += memory.Size(rec.Length)
			} else {
				logFiles[rec.Log].Used += memory.Size(rec.Length)
			}

			nset++

			return nil
		}()
		return rerr == nil, rerr
	})

	var lp []LogReport
	for _, v := range logFiles {
		lp = append(lp, *v)
	}

	slices.SortFunc(lp, func(a, b LogReport) int {
		return int(a.Unknown() - b.Unknown())
	})

	sum := LogReport{
		Path: "SUMMARY",
	}
	tbl := table.NewWriter()
	tbl.SetOutputMirror(os.Stdout)
	tbl.AppendHeader(table.Row{"ID", "Path", "TTL", "Real size", "Used", "Cleanable", "Unknown"})
	for _, v := range lp {
		sum.RealSize += v.RealSize
		sum.Used += v.Used
		sum.Expired += v.Expired
		sum.Trash += v.Trash
		ttl := ""
		if !v.TTL.IsZero() {
			ttl = v.TTL.Format(time.RFC3339)
		}
		tbl.AppendRow(table.Row{
			v.ID,
			v.Path,
			ttl,
			v.RealSize,
			v.Used,
			v.Expired,
			v.Trash,
			v.Unknown(),
		})
	}
	tbl.AppendFooter(table.Row{
		"",
		sum.Path,
		"",
		sum.RealSize,
		sum.Used,
		sum.Expired,
		sum.Trash,
		sum.Unknown(),
	})
	tbl.Render()

	return nil
}

type LogReport struct {
	ID       int
	Path     string
	RealSize memory.Size
	Used     memory.Size
	Expired  memory.Size
	Trash    memory.Size
	TTL      time.Time
}

func (r LogReport) Unknown() memory.Size {
	return r.RealSize - r.Used - r.Expired - r.Trash
}

func findFiles(dir string) (map[uint64]*LogReport, error) {
	sizes := make(map[uint64]*LogReport)
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

		var ttl time.Time
		if len(name) == 29 {
			ttlTime, err := strconv.ParseUint(name[21:], 16, 64)
			if err != nil {
				return errors.Errorf("unable to parse ttlTime=%q: %v", name, err)
			}
			if ttlTime > 0 {
				ttl = hashstore.DateToTime(uint32(ttlTime))
			}
		}

		info, err := d.Info()
		if err != nil {
			return errors.Errorf("unable to get file info for %q: %v", path, err)
		}
		sizes[id] = &LogReport{
			ID:       int(id),
			Path:     info.Name(),
			RealSize: memory.Size(info.Size()),
			TTL:      ttl,
		}

		return err
	})
	return sizes, err
}
