package hashstore

import (
	"context"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/pkg/errors"
	"os"
	"storj.io/storj/storagenode/hashstore"
	"time"
)

type Stat struct {
	Path string `arg:""`
}

func (i *Stat) Run() error {
	o, err := os.Open(i.Path)
	if err != nil {
		return errors.WithStack(err)
	}

	defer o.Close()

	ctx := context.Background()

	hashtbl, err := hashstore.OpenHashtbl(ctx, o)
	if err != nil {
		return errors.WithStack(err)
	}
	defer hashtbl.Close()

	stat := hashtbl.Stats()

	tbl := table.NewWriter()
	tbl.SetOutputMirror(os.Stdout)
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
