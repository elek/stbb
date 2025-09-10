package metabase

import (
	"context"
	"fmt"
	"os"

	access2 "github.com/elek/stbb/pkg/access"
	"github.com/elek/stbb/pkg/db"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/common/uuid"
)

type GetBucket struct {
	db.WithDatabase
	ProjectID uuid.UUID `help:"the project ID. Keep it empty and set UPLINK_ACCESS env var to use the project ID from the access grant"`
	Bucket    string    `arg:""`
}

func (s *GetBucket) Run() error {
	ctx := context.Background()
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}
	sdb, err := s.GetSatelliteDB(ctx, log)
	if err != nil {
		return errors.WithStack(err)
	}
	defer sdb.Close()

	if s.ProjectID.IsZero() && os.Getenv("UPLINK_ACCESS") != "" {
		access, err := access2.ParseAccess(os.Getenv("UPLINK_ACCESS"))
		if err != nil {
			return errors.WithStack(err)
		}
		head := access.ApiKey.Head()
		byHead, err := sdb.Console().APIKeys().GetByHead(ctx, head)
		if err != nil {
			return errors.WithStack(err)
		}
		s.ProjectID = byHead.ProjectID
	}
	bucket, err := sdb.Buckets().GetBucket(ctx, []byte(s.Bucket), s.ProjectID)
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Println("name", bucket.Name)
	fmt.Println("created", bucket.Created)
	fmt.Println("createdby", bucket.CreatedBy)
	fmt.Println("placement", bucket.Placement)
	return nil
}
