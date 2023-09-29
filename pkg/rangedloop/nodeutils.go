package rangedloop

import (
	"context"
	"github.com/pkg/errors"
	"storj.io/common/storj"
	"storj.io/common/storj/location"
	"storj.io/private/tagsql"
	"storj.io/storj/satellite/metabase"
	"time"
)

func getNodeAliasMap(conn tagsql.Conn) (map[metabase.NodeAlias]storj.NodeID, error) {
	res := make(map[metabase.NodeAlias]storj.NodeID)
	var alias metabase.NodeAlias
	var nodeIDBytes []byte

	rows, err := conn.QueryContext(context.Background(), "select node_id, node_alias from node_aliases")
	if err != nil {
		return res, errors.WithStack(err)
	}
	defer rows.Close()
	for rows.Next() {

		err := rows.Scan(&nodeIDBytes, &alias)
		if err != nil {
			return res, errors.WithStack(err)
		}
		res[alias], err = storj.NodeIDFromBytes(nodeIDBytes)
		if err != nil {
			return res, errors.WithStack(err)
		}
	}
	return res, nil
}

type Node struct {
	NodeID    storj.NodeID
	Address   string
	Email     string
	Country   location.CountryCode
	LastNet   string
	UpdatedAt time.Time
}

func getNodes(conn tagsql.Conn) (map[storj.NodeID]Node, error) {
	res := make(map[storj.NodeID]Node)

	rows, err := conn.QueryContext(context.Background(), "select id,address,email,country_code,updated_at,last_net from nodes")
	if err != nil {
		return res, errors.WithStack(err)
	}
	defer rows.Close()
	for rows.Next() {
		n := Node{}
		err := rows.Scan(&n.NodeID, &n.Address, &n.Email, &n.Country, &n.UpdatedAt, &n.LastNet)
		if err != nil {
			return res, errors.WithStack(err)
		}
		res[n.NodeID] = n
		if err != nil {
			return res, errors.WithStack(err)
		}
	}
	return res, nil
}
