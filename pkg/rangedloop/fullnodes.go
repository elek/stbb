package rangedloop

import (
	"context"
	"database/sql"
	"github.com/zeebo/errs"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/shared/location"
	"storj.io/storj/shared/tagsql"
	"time"
)

type FullSelectedNode struct {
	nodeselection.SelectedNode
	Email  string
	Wallet string
}

// GetParticipatingNodes returns all known participating nodes (this includes all known nodes
// excluding nodes that have been disqualified or gracefully exited).
func GetParticipatingNodes(ctx context.Context, db tagsql.DB) (records []*FullSelectedNode, err error) {
	var nodes []*FullSelectedNode

	err = withRows(db.QueryContext(ctx, `
		SELECT id, address, last_net, last_ip_port, country_code,
			last_contact_success > $1 AS online,
			(offline_suspended IS NOT NULL OR unknown_audit_suspended IS NOT NULL) AS suspended,
			false AS disqualified,
			exit_initiated_at IS NOT NULL AS exiting,
			false AS exited,
			email,
			wallet
		FROM nodes			
		WHERE disqualified IS NULL
			AND exit_finished_at IS NULL
	`, time.Now().Add(-12*time.Hour),
	))(func(rows tagsql.Rows) error {
		for rows.Next() {
			node, err := scanSelectedNode(rows)
			if err != nil {
				return err
			}
			nodes = append(nodes, &node)
		}
		return nil
	})
	if err != nil {
		return nil, Error.Wrap(err)
	}

	return nodes, nil
}

func scanSelectedNode(rows tagsql.Rows) (FullSelectedNode, error) {
	var node FullSelectedNode
	node.Address = &pb.NodeAddress{}
	var nodeID nullNodeID
	var address, lastNet, lastIPPort, countryCode sql.NullString
	var online, suspended, disqualified, exiting, exited sql.NullBool
	err := rows.Scan(&nodeID, &address, &lastNet, &lastIPPort, &countryCode,
		&online, &suspended, &disqualified, &exiting, &exited, &node.Email, &node.Wallet)
	if err != nil {
		return FullSelectedNode{}, err
	}

	// If node ID was null, no record was found for the specified ID. For our purposes
	// here, we will treat that as equivalent to a node being DQ'd or exited.
	if !nodeID.Valid {
		// return an empty record
		return FullSelectedNode{}, nil
	}
	// nodeID was valid, so from here on we assume all the other non-null fields are valid, per database constraints
	if disqualified.Bool || exited.Bool {
		return FullSelectedNode{}, nil
	}
	node.ID = nodeID.NodeID
	node.Address.Address = address.String
	node.LastNet = lastNet.String
	if lastIPPort.Valid {
		node.LastIPPort = lastIPPort.String
	}
	if countryCode.Valid {
		node.CountryCode = location.ToCountryCode(countryCode.String)
	}
	node.Online = online.Bool
	node.Suspended = suspended.Bool
	node.Exiting = exiting.Bool
	return node, nil
}

// nullNodeID represents a NodeID that may be null.
type nullNodeID struct {
	NodeID storj.NodeID
	Valid  bool
}

// Scan implements the sql.Scanner interface.
func (n *nullNodeID) Scan(value any) error {
	if value == nil {
		n.NodeID = storj.NodeID{}
		n.Valid = false
		return nil
	}
	err := n.NodeID.Scan(value)
	if err != nil {
		n.Valid = false
		return err
	}
	n.Valid = true
	return nil
}

// withRows ensures that rows get properly closed after the callback finishes.
func withRows(rows tagsql.Rows, err error) func(func(tagsql.Rows) error) error {
	return func(callback func(tagsql.Rows) error) error {
		if err != nil {
			return err
		}
		err := callback(rows)
		return errs.Combine(rows.Err(), rows.Close(), err)
	}
}
