package access

import (
	"fmt"
	"os"
	"storj.io/uplink"
)

type Key struct {
}

func (k Key) Run() error {
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
