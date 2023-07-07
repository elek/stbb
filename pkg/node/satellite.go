package node

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"storj.io/common/pb"
	"storj.io/common/storj"
)

type NodeStat struct {
	util.DialerHelper
	URL string `arg:""`
}

func (g NodeStat) Run() error {
	ctx := context.Background()

	dialer, err := g.CreateRPCDialer()
	if err != nil {
		return errors.WithStack(err)
	}

	nodeURL, err := storj.ParseNodeURL(g.URL)
	if err != nil {
		return errors.WithStack(err)
	}

	conn, err := dialer.DialNodeURL(ctx, nodeURL)
	if err != nil {
		return errors.WithStack(err)
	}

	statsClient := pb.NewDRPCNodeStatsClient(conn)

	model, err := statsClient.PricingModel(ctx, &pb.PricingModelRequest{})
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Println("disk_space", model.DiskSpacePrice)
	fmt.Println("egress", model.EgressBandwidthPrice)
	fmt.Println("audit", model.AuditBandwidthPrice)
	fmt.Println("repair", model.RepairBandwidthPrice)

	return nil
}
