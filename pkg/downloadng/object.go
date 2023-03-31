package downloadng

import (
	"context"
	"storj.io/common/macaroon"
	"storj.io/uplink/private/metaclient"
)

type ObjectDownloader struct {
	inbox            chan *DownloadObject
	outboxDownload   chan *DownloadPiece
	outboxEncryption chan *InitDecryption
	satelliteAddress string
	APIKey           *macaroon.APIKey
}

type DownloadObject struct {
	bucket       []byte
	encryptedKey []byte
}

func (s *ObjectDownloader) Run(ctx context.Context) error {
	defer close(s.outboxDownload)
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
			err = s.Download(ctx, metainfoClient, req)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}

	}
}

func (s *ObjectDownloader) Download(ctx context.Context, metainfoClient *metaclient.Client, req *DownloadObject) error {
	resp, err := metainfoClient.DownloadObject(ctx, metaclient.DownloadObjectParams{
		Bucket:             req.bucket,
		EncryptedObjectKey: req.encryptedKey,
	})
	if err != nil {
		return err
	}

	for _, k := range resp.DownloadedSegments {
		s.outboxEncryption <- &InitDecryption{
			bucket:               req.bucket,
			segmentEncryption:    k.Info.SegmentEncryption,
			encryptionParameters: resp.Object.EncryptionParameters,
			position:             k.Info.Position,
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
				s.outboxDownload <- &d

			}
		}
	}
	return nil
}

// this is a hack and depends on having only one segment
func (s *ObjectDownloader) Download49(ctx context.Context, metainfoClient *metaclient.Client, req *DownloadObject) error {
	used := map[int]bool{}
	for i := 0; i < 2; i++ {
		resp, err := metainfoClient.DownloadObject(ctx, metaclient.DownloadObjectParams{
			Bucket:             req.bucket,
			EncryptedObjectKey: req.encryptedKey,
		})
		if err != nil {
			return err
		}
		for _, k := range resp.DownloadedSegments {
			for ix, l := range k.Limits {
				if l != nil && l.StorageNodeAddress != nil {
					if !used[ix] {
						d := DownloadPiece{
							orderLimit: l.Limit,
							pk:         k.Info.PiecePrivateKey,
							sn:         l.StorageNodeAddress,
							size:       k.Info.EncryptedSize,
							ecShare:    ix,
							segmentID:  k.Info.SegmentID,
						}
						s.outboxDownload <- &d
						used[ix] = true
					}

				}
			}
		}
	}
	return nil
}
