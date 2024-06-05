package metabase

import (
	"context"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"os"
	"storj.io/common/storj"
	"storj.io/common/testrand"
	"storj.io/common/uuid"
	"storj.io/storj/satellite/metabase"
	"time"
)

type Inline struct {
}

func (i Inline) Run() error {
	ctx := context.Background()
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}
	expire := time.Now().Add(time.Hour)
	metabaseDB, err := metabase.Open(ctx, log.Named("metabase"), os.Getenv("STBB_DB_METAINFO"), metabase.Config{
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

	o, err := metabaseDB.BeginObjectNextVersion(ctx, metabase.BeginObjectNextVersion{
		ObjectStream: objectStream,
		ExpiresAt:    &expire,
		Encryption: storj.EncryptionParameters{
			CipherSuite: storj.EncAESGCM,
			BlockSize:   256,
		},
	})
	if err != nil {
		return errors.WithStack(err)
	}

	err = metabaseDB.CommitInlineSegment(ctx, metabase.CommitInlineSegment{
		ObjectStream:      o.ObjectStream,
		EncryptedKey:      []byte{1, 2, 3, 4},
		EncryptedKeyNonce: []byte{1, 2, 3, 4},
		ExpiresAt:         &expire,
		Position: metabase.SegmentPosition{
			Part:  uint32(0),
			Index: uint32(0),
		},

		PlainSize:     int32(0), // TODO incompatible types int32 vs int64
		EncryptedETag: nil,

		InlineData: []byte{},
	})

	_, err = metabaseDB.CommitObject(ctx, metabase.CommitObject{
		ObjectStream: o.ObjectStream,
	})

	return err
}
