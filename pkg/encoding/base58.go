package encoding

import (
	"encoding/hex"
	"fmt"
	"github.com/spf13/cobra"
	"storj.io/common/base58"
)

func init() {
	EncodingCmd.AddCommand(&cobra.Command{
		Use: "base58-decode",
		RunE: func(cmd *cobra.Command, args []string) error {
			result, _, err := base58.CheckDecode(args[0])
			if err != nil {
				return err
			}
			fmt.Println(hex.EncodeToString(result))
			return nil
		},
	})

	EncodingCmd.AddCommand(&cobra.Command{
		Use: "base58-encode",
		RunE: func(cmd *cobra.Command, args []string) error {
			parsed, err := hex.DecodeString(args[0])
			s := base58.CheckEncode(parsed, 0)
			if err != nil {
				return err
			}
			fmt.Println(s)
			return nil
		},
	})
}
