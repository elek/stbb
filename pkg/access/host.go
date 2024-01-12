package access

import (
	"fmt"
	"os"
	"storj.io/common/storj"
)

type Host struct {
	ReplacementHost string `arg:""`
	ReplacementID   string
}

func (h Host) Run() error {
	gr := os.Getenv("UPLINK_ACCESS")
	access, err := ParseAccess(gr)
	if err != nil {
		return err
	}
	id := access.SatelliteURL.ID
	if h.ReplacementID != "" {
		id, err = storj.NodeIDFromString(h.ReplacementID)
		if err != nil {
			return err
		}
	}
	access.SatelliteURL = storj.NodeURL{
		ID:      id,
		Address: h.ReplacementHost,
	}

	serialized, err := access.Serialize()
	if err != nil {
		return err
	}
	fmt.Println(serialized)
	return nil
}
