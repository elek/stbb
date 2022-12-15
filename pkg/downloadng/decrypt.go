package downloadng

import (
	"context"
	"fmt"
	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/metaclient"
)

type Decrypt struct {
	inboxInit    chan *InitDecryption
	inboxDecrypt chan *DecryptBuffer
}

type DecryptBuffer struct {
	decrypted []byte
}

type InitDecryption struct {
	segmentEncryption    metaclient.SegmentEncryption
	encryptionParameters storj.EncryptionParameters
	bucket               []byte
	unencryptedKey       []byte
	position             *metaclient.SegmentPosition
}

func NewDecrypt() (*Decrypt, error) {
	return &Decrypt{}, nil
}

func (d *Decrypt) Run(ctx context.Context) error {
	var decrypter encryption.Transformer
	for {
		select {
		case init := <-d.inboxInit:
			store := encryption.NewStore()

			derivedKey, err := encryption.DeriveContentKey(string(init.bucket), paths.NewUnencrypted("asd"), store)
			if err != nil {
				return err
			}

			ep := init.encryptionParameters
			contentKey, err := encryption.DecryptKey(init.segmentEncryption.EncryptedKey, ep.CipherSuite, derivedKey, &init.segmentEncryption.EncryptedKeyNonce)
			if err != nil {
				return err
			}

			nonce, err := deriveContentNonce(init.position.PartNumber, init.position.Index)
			if err != nil {
				return err
			}
			decrypter, err = encryption.NewDecrypter(ep.CipherSuite, contentKey, &nonce, int(ep.BlockSize))
			if err != nil {
				return err
			}
		case req := <-d.inboxDecrypt:
			out := []byte{}
			transformed, err := decrypter.Transform(out, req.decrypted, 256)
			if err != nil {
				return err
			}
			fmt.Println(len(transformed))

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
