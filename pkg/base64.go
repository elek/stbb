package stbb

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/spf13/cobra"
)

func init() {
	RootCmd.AddCommand(&cobra.Command{
		Use: "base64-decode",
		RunE: func(cmd *cobra.Command, args []string) error {
			result, err := base64.URLEncoding.DecodeString(args[0])
			if err != nil {
				return err
			}
			fmt.Println(hex.EncodeToString(result))
			return nil
		},
	})

}
