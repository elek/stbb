package node

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/storj/private/date"
	"time"
)

type Usage struct {
	util.DialerHelper
	SatelliteID storj.NodeURL `arg:""`
}

func (u Usage) Run() error {
	startDate, endDate := date.MonthBoundary(time.Now().UTC())
	// start from last day of previous month
	startDate = startDate.AddDate(0, 0, -1)

	ctx := context.Background()
	conn, err := u.Connect(ctx, u.SatelliteID)
	if err != nil {
		return err
	}

	defer func() {
		_ = conn.Close()
	}()

	client := pb.NewDRPCNodeStatsClient(conn)
	usage, err := client.DailyStorageUsage(ctx, &pb.DailyStorageUsageRequest{
		From: startDate,
		To:   endDate,
	})
	if err != nil {
		return err
	}

	for ix, r := range usage.DailyStorageUsage {
		if ix > 0 {
			fmt.Println(r.Timestamp, r.IntervalEndTime, r.AtRestTotal, r.AtRestTotal/r.IntervalEndTime.Sub(usage.DailyStorageUsage[ix-1].IntervalEndTime).Hours())
		}
	}
	return nil
}
