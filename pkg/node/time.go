package node

import (
	"context"
	"fmt"

	"github.com/elek/stbb/pkg/util"
	"github.com/zeebo/errs"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/storj"
)

type Time struct {
	URL string `arg:""`
	util.DialerHelper
}

func (p Time) Run() error {
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

	client := pb.NewDRPCNodeClient(util.NewTracedConnection(conn))
	resp, err := client.GetTime(ctx, &pb.GetTimeRequest{})
	if err != nil {
		return errs.Wrap(err)
	}
	fmt.Println(resp.GetTimestamp())
	return nil
}
