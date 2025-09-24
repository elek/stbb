package admin

import (
	"context"
	"fmt"
	"os"

	"github.com/elek/stbb/pkg/db"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/common/grant"
	"storj.io/common/storj"
	"storj.io/common/uuid"
)

type SetBucketPlacement struct {
	db.WithDatabase
	Bucket    string     `arg:"" required:"" help:"name of the bucket"`
	Placement int        `arg:"" required:"" help:"placement for the bucket"`
	ProjectID *uuid.UUID `help:"project ID (leave empty to find it from UPLINK_ACCESS)"`
}

func (s *SetBucketPlacement) Run() error {
	ctx := context.Background()

	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	satelliteDB, err := s.WithDatabase.GetSatelliteDB(ctx, log)
	if err != nil {
		return errors.WithStack(err)
	}

	if s.ProjectID == nil {
		gr := os.Getenv("UPLINK_ACCESS")
		access, err := grant.ParseAccess(gr)
		if err != nil {
			return errors.WithStack(err)
		}

		if err != nil {
			return errors.WithStack(err)
		}
		satelliteDB, err := s.GetSatelliteDB(ctx, log)

		key, err := satelliteDB.Console().APIKeys().GetByHead(ctx, access.APIKey.Head())
		if err != nil {
			return errors.WithStack(err)
		}
		s.ProjectID = &key.ProjectID
	}

	bucket, err := satelliteDB.Buckets().GetBucket(ctx, []byte(s.Bucket), *s.ProjectID)
	if err != nil {
		return errors.WithStack(err)
	}
	bucket.Placement = storj.PlacementConstraint(s.Placement)
	b, err := satelliteDB.Buckets().UpdateBucket(ctx, bucket)
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Println("placement is updated", b.ID, b.Name, b.Placement)
	return nil
}
