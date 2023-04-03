package access

import (
	"fmt"
	"os"
	"storj.io/common/storj"
)

type Host struct {
}

func (h Host) changeHost(hostNew string) error {
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
