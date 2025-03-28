package hashstore

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/pkg/errors"
	"storj.io/common/storj"
	"storj.io/storj/storagenode/hashstore"
)

type Get struct {
	WithHashtable
	ID string `arg:"" usage:"the id of the record to get"`
}

func (i *Get) Run() error {
	ctx := context.Background()

	hashtbl, close, err := i.WithHashtable.Open(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	defer close()

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
