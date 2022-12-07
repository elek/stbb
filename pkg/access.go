package stbb

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"storj.io/uplink"
)

func init() {
	RootCmd.AddCommand(&cobra.Command{
		Use: "access-change",
		RunE: func(cmd *cobra.Command, args []string) error {
			return accessChange()
		},
	})
}

func accessChange() error {
	gr := os.Getenv("UPLINK_ACCESS")
	access, err := uplink.ParseAccess(gr)
	if err != nil {
		return err
	}

	saltedUserKey, err := uplink.DeriveEncryptionKey("doesitwork", []byte("salt"))
	if err != nil {
		return err
	}

	err = access.OverrideEncryptionKey("prefixtest", "onlyforyou/", saltedUserKey)
	if err != nil {
		return err
	}
	serialized, err := access.Serialize()
	if err != nil {
		return err
	}
	fmt.Println(serialized)
	return nil

}
