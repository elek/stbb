package piece

import (
	"encoding/hex"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs/v2"
	"storj.io/storj/satellite/metabase"
)

func init() {
	cmd := cobra.Command{
		Use: "alias", RunE: func(cmd *cobra.Command, args []string) error {
			return decodeAlias(args[0])
		},
		Short: "Decode piece alias (from the condensed format, stored in db)",
	}

	PieceCmd.AddCommand(&cmd)
}

func decodeAlias(pieceHashAlias string) error {
	rawAlias, err := hex.DecodeString(pieceHashAlias)
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
