package db

import (
	"context"
	"fmt"
	"os"

	access2 "github.com/elek/stbb/pkg/access"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/common/uuid"
	"storj.io/storj/satellite/metabase"
)

type ListSegments struct {
	ProjectID uuid.UUID `help:"the project ID. Keep it empty and set UPLINK_ACCESS env var to use the project ID from the access grant"`
	StreamID  uuid.UUID `arg:""`
	WithDatabase
}

func (s *ListSegments) Run() error {
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

	result, err := metabaseDB.ListSegments(ctx, metabase.ListSegments{
		ProjectID: s.ProjectID,
		StreamID:  s.StreamID,
		Limit:     1000,
	})
	if err != nil {
		return errors.WithStack(err)
	}

	for _, segment := range result.Segments {
		fmt.Printf("%s/%d placement=%d encrypted_size=%d\n", segment.StreamID, segment.Position.Encode(), segment.Placement, segment.EncryptedSize)
	}
	return nil
}
