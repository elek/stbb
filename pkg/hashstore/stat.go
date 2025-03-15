package hashstore

import (
	"context"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"storj.io/storj/storagenode/hashstore"
	"strings"
	"time"
)

type Stat struct {
	Path string `arg:""`
}

func Open(ctx context.Context, path string) (hashstore.Tbl, func() error, error) {
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
				return Open(ctx, filepath.Join(path, entry.Name()))
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

func (i *Stat) Run() error {

	ctx := context.Background()

	hashtbl, close, err := Open(ctx, i.Path)
	if err != nil {
		return errors.WithStack(err)
	}
	defer close()

	stat := hashtbl.Stats()

	tbl := table.NewWriter()
	tbl.SetOutputMirror(os.Stdout)

	tbl.AppendRow(table.Row{
		"Kind",
		hashtbl.Header().Kind.String(),
		"type of hasthable",
	})

	tbl.AppendRow(table.Row{
		"LogSlots",
		hashtbl.Header().LogSlots,
		"number of log slots in the hash table",
	})

	tbl.AppendRow(table.Row{
		"Created",
		fmt.Sprintf("%d (%s)", stat.Created, dateToTime(stat.Created).Format(time.RFC3339)),
		"",
	})

	tbl.AppendRow(table.Row{
		"NumSet",
		stat.NumSet,
		"number of set records",
	})
	tbl.AppendRow(table.Row{
		"LenSet",
		stat.LenSet,
		"sum of lengths in set records",
	})
	tbl.AppendRow(table.Row{
		"AvgSet",
		stat.AvgSet,
		" average size of length of records",
	})

	tbl.AppendRow(table.Row{
		"NumTrash",
		stat.NumTrash,
		"number of set trash records.",
	})
	tbl.AppendRow(table.Row{
		"LenTrash",
		stat.LenTrash,
		"sum of lengths in set trash records",
	})
	tbl.AppendRow(table.Row{
		"AvgTrash",
		stat.AvgTrash,
		"average size of length of trash records",
	})

	tbl.AppendRow(table.Row{
		"NumTrash",
		stat.NumTrash,
		"total number of records available",
	})
	tbl.AppendRow(table.Row{
		"NumSlots",
		stat.NumSlots,
		"sum of lengths in set trash records",
	})
	tbl.AppendRow(table.Row{
		"TableSize",
		stat.TableSize,
		"total number of bytes in the hash table",
	})
	tbl.AppendRow(table.Row{
		"Load",
		stat.Load,
		"percent of slots that are set",
	})

	tbl.Render()
	return nil
}
