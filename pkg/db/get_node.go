package db

import (
	"context"
	"fmt"

	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/common/storj"
)

type GetNode struct {
	WithDatabase
	NodeID storj.NodeID `arg:""`
}

func (s *GetNode) Run() error {
	ctx := context.Background()
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}
	sdb, err := s.GetSatelliteDB(ctx, log)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		if err := sdb.Close(); err != nil {
			fmt.Printf("Warning: failed to close sdb: %v\n", err)
		}
	}()
	node, err := sdb.OverlayCache().Get(ctx, s.NodeID)
	if err != nil {
		return errors.WithStack(err)
	}

	util.PrintStruct(node)

	return nil
}
