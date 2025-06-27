package segment

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/access"
	"github.com/elek/stbb/pkg/db"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"io"
	"os"
	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/common/storj"
	"storj.io/common/uuid"
	"storj.io/storj/satellite/metabase"
	"storj.io/uplink/private/metaclient"
)

type Decrypt struct {
	StreamID string `arg:""`
	Bucket   string
	Key      string
	db.WithDatabase
	ProjectID uuid.UUID
}

func (s *Decrypt) Run() error {
	su, sp, err := util.ParseSegmentPosition(s.StreamID)
	if err != nil {
		return err
	}
	ctx := context.Background()

	log, err := zap.NewDevelopment()
	if err != nil {
		return err
	}

	metabaseDB, err := s.GetMetabaseDB(ctx, log)
	if err != nil {
		return errors.WithStack(err)
	}
	defer metabaseDB.Close()
	if err != nil {
		return errors.WithStack(err)
	}

	access, err := access.ParseAccess(os.Getenv("UPLINK_ACCESS"))
	if err != nil {
		return err
	}

	encryptedPath, err := encryption.EncryptPath(s.Bucket, paths.NewUnencrypted(s.Key), storj.EncAESGCM, access.EncAccess.Store)
	if err != nil {
		return errors.WithStack(err)
	}

	obj, err := metabaseDB.GetObjectLastCommitted(ctx, metabase.GetObjectLastCommitted{
		ObjectLocation: metabase.ObjectLocation{
			ProjectID:  s.ProjectID,
			BucketName: metabase.BucketName(s.Bucket),
			ObjectKey:  metabase.ObjectKey(encryptedPath.Raw()),
		},
	})
	if err != nil {
		return errors.WithStack(err)
	}

	segment, err := metabaseDB.GetSegmentByPosition(ctx, metabase.GetSegmentByPosition{
		StreamID: su,
		Position: sp,
	})
	if err != nil {
		return errors.WithStack(err)
	}

	derivedKey, err := encryption.DeriveContentKey(s.Bucket, paths.NewUnencrypted(s.Key), access.EncAccess.Store)
	if err != nil {
		return errors.WithStack(err)
	}

	startingNonce, err := deriveContentNonce(metaclient.SegmentPosition{
		PartNumber: int32(sp.Part),
		Index:      int32(sp.Index),
	})
	if err != nil {
		return errors.WithStack(err)
	}

	nonce, err := storj.NonceFromBytes(segment.EncryptedKeyNonce)
	if err != nil {
		return errors.WithStack(err)
	}
	contentKey, err := encryption.DecryptKey(segment.EncryptedKey, obj.Encryption.CipherSuite, derivedKey, &nonce)
	if err != nil {
		return err
	}

	decrypter, err := encryption.NewDecrypter(obj.Encryption.CipherSuite, contentKey, &startingNonce, int(obj.Encryption.BlockSize))
	if err != nil {
		return err
	}

	inputFile := fmt.Sprintf("segment_%s_%d.bin", su, sp.Encode())
	input, err := os.Open(inputFile)
	if err != nil {
		return errors.WithStack(err)
	}
	defer input.Close()

	outputFile := fmt.Sprintf("segment_%s_%d.decrypted", su, sp.Encode())
	output, err := os.Create(outputFile)
	if err != nil {
		return errors.WithStack(err)
	}
	defer output.Close()

	buffer := make([]byte, decrypter.InBlockSize())
	obuffer := make([]byte, decrypter.OutBlockSize())
	b := 0

	// we need to ignore the remaining part of the EC decoded segment, as it's padded
	remainingEncrypted := int(segment.EncryptedSize)

	// we also have some padding due to the encryption
	remainingPlain := int(segment.PlainSize)
	k := 0
	l := 0
	for {
		n, err := input.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break // End of file reached
			}
			return errors.WithStack(err)
		}
		decrypted, err := decrypter.Transform(obuffer[:0], buffer[:n], int64(b))
		if err != nil {
			return errors.Wrap(err, "decrypt failed at "+fmt.Sprintf("segment %s/%d, block %d, length: %d", su, sp.Encode(), b, n))
		}
		k += len(decrypted)
		l++
		if remainingPlain < len(decrypted) {
			decrypted = decrypted[:remainingPlain]
		}
		_, err = output.Write(decrypted)
		if err != nil {
			return errors.WithStack(err)
		}
		b++

		remainingEncrypted -= n
		if remainingEncrypted <= 0 {
			break
		}
		remainingPlain -= len(decrypted)
		if remainingPlain <= 0 {
			break
		}
	}

	return nil
}

func deriveContentNonce(pos metaclient.SegmentPosition) (storj.Nonce, error) {
	// The increment by 1 is to avoid nonce reuse with the metadata encryption,
	// which is encrypted with the zero nonce.
	var n storj.Nonce
	_, err := encryption.Increment(&n, int64(pos.PartNumber)<<32|(int64(pos.Index)+1))
	return n, err
}
