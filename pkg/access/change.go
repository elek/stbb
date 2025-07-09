package access

import (
	"fmt"
	"os"
	"storj.io/common/macaroon"
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
	id := access.SatelliteURL.ID

	if h.ID != "" {
		access.SatelliteURL.ID = id
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
