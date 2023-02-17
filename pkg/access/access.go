package access

import (
	"fmt"
	"github.com/elek/stbb/pkg"
	"github.com/spf13/cobra"
	"os"
	"storj.io/common/storj"
	"storj.io/uplink"
)

func init() {
	stbb.RootCmd.AddCommand(&cobra.Command{
		Use: "access-change",
		RunE: func(cmd *cobra.Command, args []string) error {
			return accessChange()
		},
	})

	stbb.RootCmd.AddCommand(&cobra.Command{
		Use: "change-host",
		RunE: func(cmd *cobra.Command, args []string) error {
			return changeHost(args[0])
		},
	})
}

func changeHost(hostNew string) error {
	gr := os.Getenv("UPLINK_ACCESS")
	access, err := ParseAccess(gr)
	if err != nil {
		return err
	}
	access.SatelliteURL = storj.NodeURL{
		ID:      access.SatelliteURL.ID,
		Address: hostNew,
	}

	serialized, err := access.Serialize()
	if err != nil {
		return err
	}
	fmt.Println(serialized)
	return nil
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
