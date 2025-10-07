package db

import (
	"context"
	"fmt"

	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type GetUser struct {
	WithDatabase
	Email string `arg:""`
}

func (s *GetUser) Run() error {
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

	user, err := sdb.Console().Users().GetByEmail(ctx, s.Email)
	if err != nil {
		return errors.WithStack(err)
	}
	util.PrintStruct(user)

	return nil
}
