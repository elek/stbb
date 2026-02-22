package access

import (
	"fmt"
	"os"

	"storj.io/common/macaroon"
	"storj.io/common/storj"
)

type Change struct {
	Host   string
	ID     string
	ApiKey string
}

func (h Change) Run() error {
	gr := os.Getenv("UPLINK_ACCESS")
	access, err := ParseAccess(gr)
	if err != nil {
		return err
	}
	if h.ID != "" {
		nodeID, err := storj.NodeIDFromString(h.ID)
		if err != nil {
			return err
		}
		access.SatelliteURL.ID = nodeID
	}

	if h.Host != "" {
		access.SatelliteURL.Address = h.Host
	}

	if h.ApiKey != "" {
		mac, err := macaroon.ParseAPIKey(h.ApiKey)
		if err != nil {
			return err
		}
		access.ApiKey = mac
	}

	serialized, err := access.Serialize()
	if err != nil {
		return err
	}
	fmt.Println(serialized)
	return nil
}
