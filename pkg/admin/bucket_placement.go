package admin

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/db"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"os"
	"storj.io/common/grant"
	"storj.io/common/storj"
)

type SetBucketPlacement struct {
	db.WithDatabase
	Bucket    string `arg:"" required:"" help:"name of the bucket"`
	Placement int    `arg:"" required:"" help:"placement for the bucket"`
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

	gr := os.Getenv("UPLINK_ACCESS")
	access, err := grant.ParseAccess(gr)
	if err != nil {
		return errors.WithStack(err)
	}

	key, err := satelliteDB.Console().APIKeys().GetByHead(ctx, access.APIKey.Head())
	if err != nil {
		return errors.WithStack(err)
	}
	bucket, err := satelliteDB.Buckets().GetBucket(ctx, []byte(s.Bucket), key.ProjectID)
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
