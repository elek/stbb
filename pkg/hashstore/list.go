package hashstore

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"storj.io/common/storj"
	"storj.io/storj/storagenode/hashstore"
	"time"
)

type List struct {
	WithHashtable
	Expired bool   `help:"list expired records" default:"true"`
	Trash   bool   `help:"list trashed records" default:"true"`
	ValidAt string `help:"list records only if valid at this time (created before, expired after)"`
	Key     bool   `help:"print only keys in PieceID format"`
}

func (i *List) Run() error {

	ctx := context.Background()
	hashtbl, close, err := i.WithHashtable.Open(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	defer close()

	var valid uint32
	if i.ValidAt != "" {

		validTime, err := time.ParseInLocation("2006-01-02", i.ValidAt, time.UTC)
		if err != nil {
			return err
		}
		valid = hashstore.TimeToDateDown(validTime)
	}

	err = hashtbl.Range(ctx, func(ctx2 context.Context, record hashstore.Record) (bool, error) {
		if !i.Trash && record.Expires.Trash() {
			return true, nil
		}
		if i.ValidAt != "" && (record.Expires.Time() <= valid || record.Created >= valid) {
			return true, nil
		}
		if i.Key {
			fmt.Println(storj.PieceID(record.Key).String())
			return true, nil
		}
		fmt.Println(record.String())
		return true, nil
	})
	return err
}
