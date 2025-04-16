package node

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"time"
)

type Checkin struct {
	util.DialerHelper
	URL             string `arg:""`
	ExternalAddress string
}

func (c *Checkin) Run() error {
	ctx := context.Background()

	dialer, err := c.CreateRPCDialer()
	if err != nil {
		return err
	}

	nodeURL, err := storj.ParseNodeURL(c.URL)
	if err != nil {
		return err
	}
	conn, err := dialer.DialNode(ctx, nodeURL, rpc.DialOptions{
		ReplaySafe: false,
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewDRPCNodeClient(conn)
	resp, err := client.CheckIn(ctx, &pb.CheckInRequest{
		Address: c.ExternalAddress,
		Capacity: &pb.NodeCapacity{
			FreeDisk: 100000000,
		},
		Operator: &pb.NodeOperator{
			Email: "asd@asd.com",
		},
		Version: &pb.NodeVersion{
			Version:   "latest & greatest",
			Timestamp: time.Now(),
		},
	})
	if err != nil {
		return err
	}
	fmt.Println(resp)
	return nil
}
