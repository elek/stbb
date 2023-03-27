package node

import (
	"encoding/csv"
	"fmt"
	"github.com/oschwald/maxminddb-golang"
	"github.com/pkg/errors"
	"net"
	"os"
	"storj.io/common/storj"
	"strings"
)

type Report struct {
	NodeFile string `arg:""`
}

func (r Report) Run() error {

	geoIP, err := maxminddb.Open("/tmp/geolite2-city/GeoLite2-City.mmdb")
	if err != nil {
		return errors.WithStack(err)
	}

	c := csv.NewWriter(os.Stdout)
	defer c.Flush()
	err = c.Write([]string{
		"id",
		"last_net",
		"country_code",
		"last_ip_port",
		"latitude",
		"longitude",
	})
	if err != nil {
		return err
	}
	return forEachNode(r.NodeFile, func(node storj.NodeURL, values map[string]string) error {
		ip := strings.Split(values["last_ip_port"], ":")[0]
		var res map[string]interface{}
		err := geoIP.Lookup(net.ParseIP(ip), &res)
		if err != nil {
			return err
		}
		var location map[string]interface{}
		if res["location"] != nil {
			location = res["location"].(map[string]interface{})
		}
		safe := func(val interface{}) string {
			if val == nil {
				return ""
			}
			return fmt.Sprintf("%f", val)
		}
		return c.Write([]string{
			node.ID.String(),
			values["last_net"],
			values["country_code"],
			values["last_ip_port"],
			safe(location["latitude"]),
			safe(location["longitude"]),
		})

	})
}
