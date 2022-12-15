package piece

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"time"
)

type OrderLimitCreator interface {
	CreateOrderLimit(ctx context.Context, pieceID storj.PieceID, size int64, satellite storj.NodeID, sn storj.NodeID) (limit *pb.OrderLimit, pk storj.PiecePrivateKey, serial storj.SerialNumber, err error)
}

type KeySigner struct {
	signer signing.Signer
	action pb.PieceAction
}

func NewKeySigner() (*KeySigner, error) {
	d := KeySigner{}
	d.action = pb.PieceAction_GET
	keysDir := os.Getenv("STBB_KEYS")
	satelliteIdentityCfg := identity.Config{
		CertPath: filepath.Join(keysDir, "identity.cert"),
		KeyPath:  filepath.Join(keysDir, "identity.key"),
	}
	id, err := satelliteIdentityCfg.Load()
	if err != nil {
		return nil, err
	}
	d.signer = signing.SignerFromFullIdentity(id)
	return &d, nil
}

func (d *KeySigner) CreateOrderLimit(ctx context.Context, pieceID storj.PieceID, size int64, satellite storj.NodeID, sn storj.NodeID) (limit *pb.OrderLimit, pk storj.PiecePrivateKey, serial storj.SerialNumber, err error) {

	pub, pk, err := storj.NewPieceKey()
	if err != nil {
		return
	}
	_, err = rand.Read(serial[:])
	if err != nil {
		return
	}

	limit = &pb.OrderLimit{
		PieceId:         pieceID,
		SerialNumber:    serial,
		SatelliteId:     satellite,
		StorageNodeId:   sn,
		Action:          d.action,
		Limit:           size,
		OrderCreation:   time.Now(),
		OrderExpiration: time.Now().Add(24 * time.Hour),
		UplinkPublicKey: pub,
	}
	limit, err = signing.SignOrderLimit(ctx, d.signer, limit)
	if err != nil {
		return
	}
	return
}

//
//func collectNodes(ctx context.Context, dialer rpc.Dialer, s string) (orderLimits map[string]downloadInfo, err error) {
//	p, err := ulloc.Parse(s)
//	if err != nil {
//		return
//	}
//	bucket, key, ok := p.RemoteParts()
//	if !ok {
//		err = errs.New("Path is not remote %s", s)
//		return
//	}
//
//	gr := os.Getenv("UPLINK_ACCESS")
//	access, err := grant.ParseAccess(gr)
//	if err != nil {
//		return
//	}
//
//	metainfoClient, err := metaclient.DialNodeURL(ctx,
//		dialer,
//		access.SatelliteAddress,
//		access.APIKey,
//		"stbb")
//	if err != nil {
//		return
//	}
//	defer metainfoClient.Close()
//
//	decoded, err := base64.URLEncoding.DecodeString(key)
//	if err != nil {
//		return
//	}
//
//	orderLimits = map[string]downloadInfo{}
//
//	for i := 0; i < 20; i++ {
//		resp, err := metainfoClient.DownloadObject(ctx, metaclient.DownloadObjectParams{
//			Bucket:             []byte(bucket),
//			EncryptedObjectKey: decoded,
//		})
//		if err != nil {
//			return orderLimits, err
//		}
//		for _, segment := range resp.DownloadedSegments {
//			for _, l := range segment.Limits {
//				if l != nil && l.StorageNodeAddress != nil {
//					nodeID := l.Limit.StorageNodeId.String()
//					if _, found := orderLimits[nodeID]; !found {
//						orderLimits[nodeID] = downloadInfo{
//							PrivateKey:  segment.Info.PiecePrivateKey,
//							Limit:       l.Limit,
//							NodeAddress: l.StorageNodeAddress,
//						}
//					}
//				}
//			}
//		}
//	}
//	return
//}
