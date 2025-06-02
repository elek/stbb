package node

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/db"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/common/storj"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/overlay"
	"strings"
)

type Info struct {
	db.WithDatabase
	NodeID   storj.NodeID `arg:""`
	Selector string
}

func (i Info) Run() error {
	ctx := context.Background()
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	satelliteDB, err := i.WithDatabase.GetSatelliteDB(ctx, log)

	if err != nil {
		return err
	}
	defer satelliteDB.Close()
	node, err := satelliteDB.OverlayCache().Get(ctx, i.NodeID)
	if err != nil {
		return errors.WithStack(err)
	}
	tags, err := satelliteDB.OverlayCache().GetNodeTags(ctx, node.Id)
	if err != nil {
		return errors.WithStack(err)
	}
	if i.Selector != "" {
		var attributes []nodeselection.NodeAttribute
		for _, attr := range strings.Split(i.Selector, ",") {
			attr, err := nodeselection.CreateNodeAttribute(attr)
			if err != nil {
				return errors.WithStack(err)
			}
			attributes = append(attributes, attr)
		}
		nodeAttribute := func(n nodeselection.SelectedNode) string {
			var result []string
			for _, attr := range attributes {
				result = append(result, attr(n))
			}
			return strings.Join(result, ",")
		}
		sn := nodeselection.SelectedNode{
			ID:          node.Id,
			Address:     node.Address,
			LastNet:     node.LastNet,
			LastIPPort:  node.LastIPPort,
			CountryCode: node.CountryCode,
		}
		for _, tag := range tags {
			sn.Tags = append(sn.Tags, nodeselection.NodeTag{
				NodeID:   node.Id,
				Name:     tag.Name,
				Value:    tag.Value,
				SignedAt: node.CreatedAt,
				Signer:   node.Id,
			})
		}
		fmt.Println(nodeAttribute(sn))
		return nil
	}
	fmt.Println("free disk", node.Capacity.FreeDisk)
	fmt.Println("address", node.Address.Address)
	fmt.Println("country_code", node.CountryCode)
	fmt.Println("last_net", node.LastNet)
	fmt.Println("last_ip_port", node.LastIPPort)
	fmt.Println("piece_count", node.PieceCount)
	fmt.Println("version", node.Version)
	fmt.Println("dq", node.Disqualified)
	if node.DisqualificationReason != nil {
		switch *node.DisqualificationReason {
		case overlay.DisqualificationReasonUnknown:
			fmt.Println("dq_reason", "unknown")
		case overlay.DisqualificationReasonAuditFailure:
			fmt.Println("dq_reason", "audit_failure")
		case overlay.DisqualificationReasonSuspension:
			fmt.Println("dq_reason", "suspension")
		case overlay.DisqualificationReasonNodeOffline:
			fmt.Println("dq_reason", "offline")
		default:
			fmt.Println("dq_reason", "???")
		}

	}
	fmt.Println("suspended", node.OfflineSuspended)

	for _, t := range tags {
		fmt.Printf("   %s=%s\n", t.Name, string(t.Value))
	}
	return nil
}
