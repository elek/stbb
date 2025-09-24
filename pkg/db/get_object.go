package db

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/common/grant"
	"storj.io/common/uuid"
	"storj.io/storj/satellite/metabase"
)

type GetObject struct {
	WithDatabase
	ProjectID     *uuid.UUID `help:"project ID (leave empty to find it from UPLINK_ACCESS)"`
	Bucket        string     `arg:""`
	EncryptedPath string     `arg:""`
}

func (s *GetObject) Run() error {
	ctx := context.Background()
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}
	metabaseDB, err := s.GetMetabaseDB(ctx, log)
	if err != nil {
		return errors.WithStack(err)
	}
	defer metabaseDB.Close()

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
	decodeString, err := hex.DecodeString(s.EncryptedPath)
	if err != nil {
		return errors.WithStack(err)
	}

	committed, err := metabaseDB.GetObjectLastCommitted(ctx, metabase.GetObjectLastCommitted{
		ObjectLocation: metabase.ObjectLocation{
			ProjectID:  *s.ProjectID,
			BucketName: metabase.BucketName(s.Bucket),
			ObjectKey:  metabase.ObjectKey(decodeString),
		},
	})
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Println("project_id", committed.ProjectID)
	fmt.Println("bucket_name", committed.BucketName)
	fmt.Println("stream_id", committed.StreamID)
	fmt.Println("stream_version_id", committed.StreamVersionID())
	fmt.Println("stream_id", committed.StreamID)
	return nil
}
