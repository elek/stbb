package node

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/db"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/common/storj"
)

type Info struct {
	db.WithDatabase
	NodeID storj.NodeID `arg:""`
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
	fmt.Println("free disk", node.Capacity.FreeDisk)
	fmt.Println("address", node.Address.Address)
	fmt.Println("country_code", node.CountryCode)
	fmt.Println("last_net", node.LastNet)
	fmt.Println("last_ip_port", node.LastIPPort)
	fmt.Println("piece_count", node.PieceCount)
	tags, err := satelliteDB.OverlayCache().GetNodeTags(ctx, node.Id)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, t := range tags {
		fmt.Printf("   %s=%s\n", t.Name, string(t.Value))
	}
	return nil
}
