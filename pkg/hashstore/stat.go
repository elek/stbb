package hashstore

import (
	"context"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/pkg/errors"
	"os"
	"time"
)

type Stat struct {
	WithHashtable
}

func (i *Stat) Run() error {

	ctx := context.Background()

	hashtbl, close, err := i.WithHashtable.Open(ctx)
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
		stat.LenSet.Base10String(),
		"sum of lengths in set records",
	})
	tbl.AppendRow(table.Row{
		"AvgSet",
		stat.AvgSet,
		"average size of length of records",
	})

	tbl.AppendRow(table.Row{
		"NumTrash",
		stat.NumTrash,
		"number of set trash records.",
	})
	tbl.AppendRow(table.Row{
		"LenTrash",
		stat.LenTrash.Base10String(),
		"sum of lengths in set trash records",
	})
	tbl.AppendRow(table.Row{
		"AvgTrash",
		stat.AvgTrash,
		"average size of length of trash records",
	})

	tbl.AppendRow(table.Row{
		"TableSize",
		stat.TableSize.Base10String(),
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
