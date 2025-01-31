package hashstore

import (
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
	hashtbl, err := hashstore.OpenHashtbl(o)
	if err != nil {
		return errors.WithStack(err)
	}
	defer hashtbl.Close()
	hashtbl.Range(func(record hashstore.Record, err error) bool {
		fmt.Println(record.String())
		return true
	})
	return nil
}
