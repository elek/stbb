package db

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/common/uuid"
)

type ListProjects struct {
	WithDatabase
	ProjectID     uuid.UUID `arg:""`
	Bucket        string    `arg:""`
	EncryptedPath string    `arg:""`
}

func (s *ListProjects) Run() error {
	ctx := context.Background()
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}
	satelliteDB, err := s.GetSatelliteDB(ctx, log)
	if err != nil {
		return errors.WithStack(err)
	}
	defer satelliteDB.Close()

	list, err := satelliteDB.Console().Projects().List(ctx, 0, 100, time.Now())
	if err != nil {
		return errors.WithStack(err)
	}
	for _, p := range list.Projects {
		fmt.Println(p.ID, p.Name)
	}
	return nil
}
