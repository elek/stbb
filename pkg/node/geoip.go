package node

import (
	"encoding/json"
	"fmt"
	"github.com/oschwald/maxminddb-golang"
	"github.com/pkg/errors"
	"net"
	"time"
)

type GeoIP struct {
	GetDatabase string `arg:""`
	IP          string `arg:""`
}

func (g GeoIP) Run() error {
	geoIP, err := maxminddb.Open(g.GetDatabase)
	if err != nil {
		return errors.WithStack(err)
	}
	defer geoIP.Close()

	ipInfo := ipInfo{}
	ip := net.ParseIP(g.IP)
	if len(ip) == 0 {
		return errors.New("Wrong IP")
	}

	for k, v := range geoIP.Metadata.Description {
		fmt.Println(k, v)
	}

	buildTime := time.Unix(int64(geoIP.Metadata.BuildEpoch), 0)
	fmt.Println(buildTime)
	err = geoIP.Lookup(ip, &ipInfo)
	if err != nil {
		return errors.WithStack(err)
	}

	raw, err := json.MarshalIndent(ipInfo, "", "  ")
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Println(g.IP)
	fmt.Println(string(raw))
	return nil
}
