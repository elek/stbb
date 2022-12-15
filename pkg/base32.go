package stbb

import (
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"github.com/spf13/cobra"
)

func init() {
	RootCmd.AddCommand(&cobra.Command{
		Use: "base32-decode",
		RunE: func(cmd *cobra.Command, args []string) error {
			result, err := base32.StdEncoding.DecodeString(args[0])
			if err != nil {
				return err
			}
			fmt.Println(hex.EncodeToString(result))
			return nil
		},
	})

	RootCmd.AddCommand(&cobra.Command{
		Use: "base32-encode",
		RunE: func(cmd *cobra.Command, args []string) error {

			raw, err := hex.DecodeString(args[0])
			if err != nil {
				return err
			}
			encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw)
			if err != nil {
				return err
			}
			fmt.Println(encoded)
			return nil
		},
	})
}
