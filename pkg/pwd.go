package stbb

import (
	"encoding/hex"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs/v2"
	"golang.org/x/crypto/bcrypt"
)

func init() {
	RootCmd.AddCommand(&cobra.Command{
		Use: "pwd",
		RunE: func(cmd *cobra.Command, args []string) error {
			password, err := bcrypt.GenerateFromPassword([]byte(""), 0)
			if err != nil {
				return errs.Wrap(err)
			}
			fmt.Println(hex.EncodeToString(password))
			return nil
		},
	})
}
