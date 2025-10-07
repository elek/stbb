package node

import (
	"context"

	"github.com/elek/stbb/pkg/util"
	"github.com/zeebo/errs"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/storj"
)

type Stats struct {
	URL string `arg:""`
	util.DialerHelper
}

func (p Stats) Run() error {
	ctx := context.Background()

	dialer, err := p.CreateRPCDialer()
	nodeURL, err := storj.ParseNodeURL(p.URL)
	if err != nil {
		return err
	}
	conn, err := dialer.DialNode(ctx, nodeURL, rpc.DialOptions{
		ReplaySafe: true,
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewDRPCNodeStatsClient(util.NewTracedConnection(conn))
	resp, err := client.GetStats(ctx, &pb.GetStatsRequest{})
	if err != nil {
		return errs.Wrap(err)
	}
	util.PrintStruct(resp)
	return nil
}
