package piece

import (
	"encoding/hex"
	"fmt"
	"github.com/pkg/errors"
	"storj.io/storj/satellite/metabase"
)

type Unalias struct {
	Hex string `arg:""`
}

func (u Unalias) Run() error {
	decodeString, err := hex.DecodeString(u.Hex)
	if err != nil {
		return errors.WithStack(err)
	}
	aliases := metabase.AliasPieces{}
	err = aliases.SetBytes(decodeString)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, a := range aliases {
		fmt.Println(a.Number, a.Alias)
	}
	return nil
}
