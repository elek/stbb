package db

import (
	"context"
	"os"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/common/storj"
	"storj.io/common/testrand"
	"storj.io/common/uuid"
	"storj.io/storj/satellite/metabase"
)

type BeginObject struct {
	Database string
}

func (i BeginObject) Run() error {
	ctx := context.Background()
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}
	expire := time.Now().Add(time.Hour)
	if i.Database == "" && os.Getenv("STBB_DB_METAINFO") != "" {
		i.Database = os.Getenv("STBB_DB_METAINFO")
	}
	metabaseDB, err := metabase.Open(ctx, log.Named("metabase"), i.Database, metabase.Config{
		ApplicationName:  "stbb",
		MaxNumberOfParts: 10,
	})
	projectId := uuid.UUID{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xCA, 0xFE, 0xBA, 0xBE}

	if err != nil {
		return errors.WithStack(err)
	}

	objectStream := metabase.ObjectStream{
		ProjectID:  projectId,
		BucketName: "bucket",
		ObjectKey:  metabase.ObjectKey("test/" + testrand.UUID().String()),
		Version:    metabase.NextVersion,
		StreamID:   testrand.UUID(),
	}

	_, err = metabaseDB.BeginObjectNextVersion(ctx, metabase.BeginObjectNextVersion{
		ObjectStream: objectStream,
		ExpiresAt:    &expire,
		Encryption: storj.EncryptionParameters{
			CipherSuite: storj.EncAESGCM,
			BlockSize:   256,
		},
	})

	return err
}
