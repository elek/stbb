package util

import (
	"context"
	"crypto/rand"
	"fmt"
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

func NewKeySignerFromFullIdentity(id *identity.FullIdentity, action pb.PieceAction) *KeySigner {
	return &KeySigner{
		signer: signing.SignerFromFullIdentity(id),
		nodeID: id.ID,
		Action: action,
	}
}
func NewKeySignerFromDir(keysDir string) (*KeySigner, error) {
	d := KeySigner{}
	d.Action = pb.PieceAction_GET

	var id *identity.FullIdentity
	keyPath := filepath.Join(keysDir, "identity.key")
	if _, err := os.Stat(keyPath); err == nil {
		satelliteIdentityCfg := identity.Config{
			CertPath: filepath.Join(keysDir, "identity.cert"),
			KeyPath:  keyPath,
		}
		id, err = satelliteIdentityCfg.Load()
		if err != nil {
			return nil, err
		}
	} else {
		fmt.Println("identity.key is missing (and not specified with STBB_KEYS) using internal one")
		id, err = identity.FullIdentityFromPEM(Certificate, Key)
		if err != nil {
			return nil, err
		}
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
