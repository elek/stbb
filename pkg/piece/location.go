package piece

import (
	"encoding/hex"
	"fmt"
	"github.com/zeebo/errs/v2"
	"storj.io/storj/satellite/metabase"
)

type Decode struct {
	PieceAlias string `args:""`
}

func (d *Decode) decodeAlias() error {
	rawAlias, err := hex.DecodeString(d.PieceAlias)
	if err != nil {
		return errs.Wrap(err)
	}
	a := metabase.AliasPieces{}

	err = a.SetBytes(rawAlias)
	if err != nil {
		return errs.Wrap(err)
	}
	for _, ap := range a {
		fmt.Printf("%d %d\n", ap.Alias, ap.Number)
	}
	return nil
}
