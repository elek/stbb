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
	fmt.Println(node.Capacity)
	fmt.Println(node.Address)
	fmt.Println(node.CountryCode)
	return nil
}
