package node

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/oschwald/maxminddb-golang"
	"github.com/zeebo/errs"
	"net"
	"os"
	"storj.io/common/storj"
	"storj.io/common/storj/location"
	"storj.io/storj/satellite/metabase"
	"time"
)

type Export struct {
	MaxmindDB string
}

func (c Export) Run() error {
	writers := make(map[metabase.NodeAlias]writer)
	defer func() {
		for _, w := range writers {
			_ = w.Close()
		}
	}()

	satelliteConn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL_SATELLITE"))
	if err != nil {
		return errs.Wrap(err)
	}
	defer satelliteConn.Close(context.Background())
	nodes, err := getNodes(satelliteConn)
	if err != nil {
		return errs.Wrap(err)
	}

	geoIP, err := maxminddb.Open(c.MaxmindDB)
	if err != nil {
		return errs.Wrap(err)
	}

	eq := func(a string, b string) bool {
		return a == b || a == "" || b == ""
	}
	defer geoIP.Close()
	k := 0
	for _, n := range nodes {
		ipInfo := ipInfo{}
		ip, err := addressToIP(n.LastIPPort)
		if err == nil {
			_ = geoIP.Lookup(ip, &ipInfo)
		}

		if !eq(ipInfo.Country.IsoCode, ipInfo.RegisteredCountry.IsoCode) {
			k++
			fmt.Println(n.NodeID, n.Address, n.LastIPPort, ipInfo.Country, ipInfo.RegisteredCountry, ipInfo.RepresentedCountry)
		}
	}
	fmt.Println(k)
	return nil
}

type ipInfo struct {
	Country struct {
		IsoCode string `maxminddb:"iso_code"`
	} `maxminddb:"country"`
	RepresentedCountry struct {
		IsoCode string `maxminddb:"iso_code"`
	} `maxminddb:"represented_country"`
	RegisteredCountry struct {
		IsoCode string `maxminddb:"iso_code"`
	} `maxminddb:"registered_country"`
}

type NodeRecord struct {
	NodeID     storj.NodeID
	Address    string
	LastIPPort string
	Country    location.CountryCode
	UpdatedAt  time.Time
}

func getNodes(conn *pgx.Conn) (map[storj.NodeID]NodeRecord, error) {
	res := make(map[storj.NodeID]NodeRecord)

	rows, err := conn.Query(context.Background(), "select id,address,country_code,updated_at,last_ip_port from nodes")
	if err != nil {
		return res, errs.Wrap(err)
	}
	defer rows.Close()
	for rows.Next() {
		n := NodeRecord{}
		err := rows.Scan(&n.NodeID, &n.Address, &n.Country, &n.UpdatedAt, &n.LastIPPort)
		if err != nil {
			return res, errs.Wrap(err)
		}
		res[n.NodeID] = n
		if err != nil {
			return res, errs.Wrap(err)
		}
	}
	return res, nil
}

func addressToIP(address string) (net.IP, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	ip := net.ParseIP(host)
	if len(ip) == 0 {
		return nil, nil
	}

	return ip, nil
}
