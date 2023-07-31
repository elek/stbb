package downloadng

import (
	"context"
	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/metaclient"
)

type Decrypt struct {
	inbox   chan any
	outbox  chan any
	store   *encryption.Store
	counter int64
}

type DecryptBuffer struct {
	encrypted []byte
}

type InitDecryption struct {
	segmentEncryption    metaclient.SegmentEncryption
	encryptionParameters storj.EncryptionParameters
	bucket               string
	unencryptedKey       string
	encryptedKey         []byte
	position             *metaclient.SegmentPosition
}

func NewDecrypt(inbox chan any, store *encryption.Store) (*Decrypt, error) {
	return &Decrypt{
		store:  store,
		inbox:  inbox,
		outbox: logReceived("final", make(chan any)),
	}, nil
}

func (d *Decrypt) Run(ctx context.Context) error {
	var decrypter encryption.Transformer
	for {
		select {
		case req := <-d.inbox:
			switch r := req.(type) {
			case *InitDecryption:
				derivedKey, err := encryption.DeriveContentKey(string(r.bucket), paths.NewUnencrypted(r.unencryptedKey), d.store)
				if err != nil {
					return err
				}

				ep := r.encryptionParameters
				contentKey, err := encryption.DecryptKey(r.segmentEncryption.EncryptedKey, ep.CipherSuite, derivedKey, &r.segmentEncryption.EncryptedKeyNonce)
				if err != nil {
					return err
				}

				nonce, err := deriveContentNonce(r.position.PartNumber, r.position.Index)
				if err != nil {
					return err
				}

				decrypter, err = encryption.NewDecrypter(ep.CipherSuite, contentKey, &nonce, int(ep.BlockSize))
				if err != nil {
					return err
				}

				d.counter = 0
			case *DecryptBuffer:
				var out []byte
				transformed, err := decrypter.Transform(out, r.encrypted, d.counter)
				if err != nil {
					return err
				}
				d.counter++
				d.outbox <- transformed
			case Done:
				d.outbox <- req
				return nil
			}
		case <-ctx.Done():
			return nil
		}

	}
}

// getEncryptedKeyAndNonce returns key and nonce directly if exists, otherwise try to get them from SegmentMeta.
func getEncryptedKeyAndNonce(metadataKey []byte, metadataNonce storj.Nonce, m *pb.SegmentMeta) (storj.EncryptedPrivateKey, *storj.Nonce) {
	if len(metadataKey) > 0 {
		return storj.EncryptedPrivateKey(metadataKey), &metadataNonce
	}

	if m == nil {
		return nil, nil
	}

	var nonce storj.Nonce
	copy(nonce[:], m.KeyNonce)

	return m.EncryptedKey, &nonce
}

func deriveContentNonce(part int32, index int32) (storj.Nonce, error) {
	// The increment by 1 is to avoid nonce reuse with the metadata encryption,
	// which is encrypted with the zero nonce.
	var n storj.Nonce
	_, err := encryption.Increment(&n, int64(part)<<32|(int64(index)+1))
	return n, err
}
