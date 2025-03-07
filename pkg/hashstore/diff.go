package hashstore

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"os"
	"storj.io/storj/storagenode/hashstore"
)

type Diff struct {
	Left  string `arg:"" help:"left hashstore"`
	Right string `arg:"" help:"right hashstore"`
}

func (d Diff) Run() error {
	ctx := context.Background()
	fmt.Println("Loading", d.Left)
	f, err := os.Open(d.Left)
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()
	leftTable, err := hashstore.OpenHashtbl(ctx, f)
	if err != nil {
		return errors.WithStack(err)
	}
	defer leftTable.Close()

	fmt.Println("Loading", d.Right)
	g, err := os.Open(d.Right)
	if err != nil {
		return errors.WithStack(err)
	}
	defer g.Close()
	rightTable, err := hashstore.OpenHashtbl(ctx, g)
	if err != nil {
		return errors.WithStack(err)
	}
	defer rightTable.Close()

	fmt.Println("hashtables are loaded")
	err = rightTable.Range(ctx, func(ctx context.Context, record hashstore.Record) (bool, error) {
		mon.Counter("diff_check").Inc(1)
		leftRecord, found, err := leftTable.Lookup(ctx, record.Key)
		if err != nil {
			return false, errors.WithStack(err)
		}
		if found {
			fmt.Println("left", leftRecord.String())
			fmt.Println("right", record.String())

		}
		return true, nil
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
