package hashstore

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"os"
	"storj.io/storj/storagenode/hashstore"
)

type List struct {
	Path string `arg:""`
}

func (i *List) Run() error {
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
	err = hashtbl.Range(ctx, func(ctx2 context.Context, record hashstore.Record) (bool, error) {
		fmt.Println(record.String())
		return true, nil
	})
	return err
}
