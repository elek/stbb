package main

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/storj/storagenode/monitor"
)

type DiskSpace struct {
}

func (d DiskSpace) Run() error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}
	ctx := context.Background()
	disk := monitor.NewDedicatedDisk(log, ".", 500_000_000_000, 100_000_000)
	dp, err := disk.DiskSpace(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	fmt.Println("Total", dp.Total)
	fmt.Println("Allocated", dp.Allocated)
	fmt.Println("Free", dp.Free)
	return nil
}
