package node

import (
	"encoding/hex"
	"fmt"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/orders"
	"storj.io/storj/storagenode/orders/ordersfile"
)

type UnsentOrder struct {
	File           string `arg:""`
	EncryptionKeys string
}

func (c UnsentOrder) Run() error {
	input, err := ordersfile.OpenReadable(c.File, ordersfile.V1)
	if err != nil {
		return err
	}
	for {
		one, err := input.ReadOne()
		if err != nil {
			return err
		}

		keys := orders.EncryptionKeys{}
		err = keys.Set(c.EncryptionKeys)
		if err != nil {
			return err
		}

		fmt.Printf("%x %d %x %x\n", one.Limit.PieceId, one.Limit.Action, one.Limit.EncryptedMetadataKeyId, one.Limit.EncryptedMetadata)

		var orderKeyID orders.EncryptionKeyID
		copy(orderKeyID[:], one.Limit.EncryptedMetadataKeyId)

		key := orders.EncryptionKey{
			ID:  orderKeyID,
			Key: keys.KeyByID[orderKeyID],
		}

		decrypted, err := key.DecryptMetadata(one.Limit.SerialNumber, one.Limit.EncryptedMetadata)
		if err != nil {
			return err
		}
		fmt.Println(hex.EncodeToString(decrypted.GetBucketId()))
		fmt.Println(hex.EncodeToString(decrypted.ProjectBucketPrefix))
		compact, err := metabase.ParseCompactBucketPrefix(decrypted.CompactProjectBucketPrefix)
		if err != nil {
			return err
		}
		fmt.Println(compact.ProjectID.String())
		fmt.Println(compact.BucketName)
	}
	return nil
}
