package hashstore

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/pkg/errors"
	"os"
	"storj.io/common/storj"
	"storj.io/storj/storagenode/hashstore"
)

type Get struct {
	Path string `arg:"" usage:"the path to the hashstore"`
	ID   string `arg:"" usage:"the id of the record to get"`
}

func (i *Get) Run() error {
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

	var pieceID storj.PieceID
	idh, err := hex.DecodeString(i.ID)
	if err == nil {
		pieceID, err = storj.PieceIDFromBytes(idh)
		if err != nil {
			return errors.WithStack(err)
		}
	} else {
		pieceID, err = storj.PieceIDFromString(i.ID)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	rec, ok, err := hashtbl.Lookup(ctx, hashstore.Key(pieceID))
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Println(ok, rec)
	return err
}
