package node

import (
	"context"
	"fmt"
	"time"

	"github.com/elek/stbb/pkg/util"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/storj"
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
			FreeDisk: 10000000000,
		},
		Operator: &pb.NodeOperator{
			Email: "stbb@storj.io",
		},
		Version: &pb.NodeVersion{
			Version:   "1.200.0",
			Timestamp: time.Now(),
		},
	})
	if err != nil {
		return err
	}
	fmt.Println("err:", resp.PingErrorMessage)
	if resp.HashstoreSettings != nil {
		fmt.Println("hashstore")
		fmt.Println("active migrate:", resp.HashstoreSettings.ActiveMigrate)
		fmt.Println("passive migrate", resp.HashstoreSettings.PassiveMigrate)
		fmt.Println("read new first", resp.HashstoreSettings.ReadNewFirst)
		fmt.Println("write to new", resp.HashstoreSettings.WriteToNew)
		fmt.Println("read new first", resp.HashstoreSettings.ReadNewFirst)

	}
	return nil
}
