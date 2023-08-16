package main

import (
	"encoding/json"
	"fmt"
	"github.com/oschwald/maxminddb-golang"
	"github.com/zeebo/errs"
	"net"
)

type GeoIP struct {
	MaxmindFile string `arg:""`
	IP          string `arg:""`
}

func (g GeoIP) Run() error {
	ip := net.ParseIP(g.IP)

	if len(ip) == 0 {
		return errs.New("Wrong IP")
	}

	mdb, err := maxminddb.Open(g.MaxmindFile)
	if err != nil {
		return errs.Wrap(err)
	}
	k := map[string]interface{}{}
	err = mdb.Lookup(ip, &k)
	if err != nil {
		return errs.Wrap(err)
	}
	pretty, err := json.MarshalIndent(k, "", "  ")
	if err != nil {
		return errs.Wrap(err)
	}
	fmt.Println(string(pretty))
	return nil
}
