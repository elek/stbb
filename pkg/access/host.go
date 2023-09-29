package access

import (
	"fmt"
	"os"
	"storj.io/common/storj"
)

type Host struct {
	ReplacementHost string `arg:""`
}

func (h Host) Run() error {
	gr := os.Getenv("UPLINK_ACCESS")
	access, err := ParseAccess(gr)
	if err != nil {
		return err
	}
	access.SatelliteURL = storj.NodeURL{
		ID:      access.SatelliteURL.ID,
		Address: h.ReplacementHost,
	}

	serialized, err := access.Serialize()
	if err != nil {
		return err
	}
	fmt.Println(serialized)
	return nil
}
