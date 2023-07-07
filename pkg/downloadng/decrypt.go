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
	store        *encryption.Store
	counter      int64
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

func NewDecrypt(store *encryption.Store) (*Decrypt, error) {
	return &Decrypt{
		store:        store,
		inboxDecrypt: make(chan *DecryptBuffer),
		inboxInit:    make(chan *InitDecryption),
	}, nil
}

func (d *Decrypt) Run(ctx context.Context) error {
	var decrypter encryption.Transformer
	for {
		select {
		case init := <-d.inboxInit:
			derivedKey, err := encryption.DeriveContentKey(string(init.bucket), paths.NewUnencrypted(init.unencryptedKey), d.store)
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

			fmt.Println(ep.BlockSize)
			decrypter, err = encryption.NewDecrypter(ep.CipherSuite, contentKey, &nonce, int(ep.BlockSize))
			if err != nil {
				return err
			}

			encPath, err := encryption.EncryptPathWithStoreCipher(init.bucket, paths.NewUnencrypted(init.unencryptedKey), d.store)
			if err != nil {
				return err
			}

			err = d.store.Add(string(init.bucket), paths.NewUnencrypted(string(init.unencryptedKey)), encPath, *contentKey)
			if err != nil {
				return err
			}
			d.counter = 0
		case req := <-d.inboxDecrypt:
			fmt.Println("Decrypting")
			var out []byte
			transformed, err := decrypter.Transform(out, req.encrypted, d.counter)
			if err != nil {
				return err
			}
			d.counter++
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
