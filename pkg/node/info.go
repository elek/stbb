package node

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"os"
	"storj.io/common/storj"
	"storj.io/storj/satellite/satellitedb"
)

type Info struct {
	NodeID storj.NodeID `arg:""`
}

func (i Info) Run() error {
	ctx := context.Background()
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	satelliteDB, err := satellitedb.Open(ctx, log.Named("metabase"), os.Getenv("STBB_DB_SATELLITE"), satellitedb.Options{
		ApplicationName: "stbb",
	})

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
