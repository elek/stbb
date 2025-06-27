package satellitedb

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/access"
	"github.com/elek/stbb/pkg/db"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"os"
)

type ApiKey struct {
	db.WithDatabase
}

func (s *ApiKey) Run() error {
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

	access, err := access.ParseAccess(os.Getenv("UPLINK_ACCESS"))
	if err != nil {
		return err
	}

	ak, err := satelliteDB.Console().APIKeys().GetByHead(ctx, access.ApiKey.Head())
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Println("id", ak.ID)
	fmt.Println("project_id", ak.ProjectID)
	return nil
}
