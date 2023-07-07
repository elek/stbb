package downloadng

import (
	"context"
	"storj.io/common/encryption"
	"storj.io/common/macaroon"
	"storj.io/common/paths"
	"storj.io/uplink/private/metaclient"
)

type ObjectDownloader struct {
	inbox            chan any
	outbox           chan any
	satelliteAddress string
	APIKey           *macaroon.APIKey
	store            *encryption.Store
}

type DownloadObject struct {
	bucket string
	key    string
}

func (s *ObjectDownloader) Run(ctx context.Context) error {
	defer close(s.outbox)
	dialer, err := getDialer(ctx, false)
	if err != nil {
		return err
	}
	metainfoClient, err := metaclient.DialNodeURL(ctx,
		dialer,
		s.satelliteAddress,
		s.APIKey,
		"stbb")
	for {
		select {
		case req := <-s.inbox:
			if req == nil {
				return nil
			}
			switch r := req.(type) {
			case *DownloadObject:
				err = s.Download(ctx, metainfoClient, r)
				if err != nil {
					return err
				}
			}

		case <-ctx.Done():
			return nil
		}

	}
}

func (s *ObjectDownloader) Download(ctx context.Context, metainfoClient *metaclient.Client, req *DownloadObject) error {

	encPath, err := encryption.EncryptPathWithStoreCipher(req.bucket, paths.NewUnencrypted(req.key), s.store)
	if err != nil {
		return err
	}
	resp, err := metainfoClient.DownloadObject(ctx, metaclient.DownloadObjectParams{
		Bucket:             []byte(req.bucket),
		EncryptedObjectKey: []byte(encPath.Raw()),
	})
	if err != nil {
		return err
	}

	for _, k := range resp.DownloadedSegments {
		s.outbox <- &InitDecryption{
			bucket:               req.bucket,
			segmentEncryption:    k.Info.SegmentEncryption,
			encryptionParameters: resp.Object.EncryptionParameters,
			position:             k.Info.Position,
			unencryptedKey:       req.key,
		}
		for ix, l := range k.Limits {
			if l != nil && l.StorageNodeAddress != nil {
				d := DownloadPiece{
					orderLimit: l.Limit,
					pk:         k.Info.PiecePrivateKey,
					sn:         l.StorageNodeAddress,
					size:       k.Info.EncryptedSize,
					ecShare:    ix,
					segmentID:  k.Info.SegmentID,
				}
				s.outbox <- &d

			}
		}
	}
	return nil
}
