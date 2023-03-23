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
	Action pb.PieceAction
	nodeID storj.NodeID
}

func NewKeySigner() (*KeySigner, error) {
	keysDir := os.Getenv("STBB_KEYS")
	return NewKeySignerFromDir(keysDir)
}

func NewKeySignerFromDir(keysDir string) (*KeySigner, error) {
	d := KeySigner{}
	d.Action = pb.PieceAction_GET

	satelliteIdentityCfg := identity.Config{
		CertPath: filepath.Join(keysDir, "identity.cert"),
		KeyPath:  filepath.Join(keysDir, "identity.key"),
	}
	id, err := satelliteIdentityCfg.Load()
	if err != nil {
		return nil, err
	}
	d.nodeID = id.ID
	d.signer = signing.SignerFromFullIdentity(id)
	return &d, nil
}

func (d *KeySigner) GetSatelliteID() storj.NodeID {
	return d.nodeID
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
		Action:          d.Action,
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
