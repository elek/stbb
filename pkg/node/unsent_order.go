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

		if len(one.Limit.EncryptedMetadataKeyId) == 0 {
			continue
		}

		fmt.Printf("piece_id=%s action=%d %x\n", hex.EncodeToString(one.Limit.PieceId[:]), one.Limit.Action, one.Limit.EncryptedMetadataKeyId)
		fmt.Println("limit.action", one.Limit.Action)
		fmt.Println("limit.satellite", one.Limit.SatelliteId)
		fmt.Println("limit.creation", one.Limit.OrderCreation)
		fmt.Println("limit.limit", one.Limit.Limit)
		fmt.Println("order.amount", one.Order.Amount)
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
		fmt.Println("metadata.bucket_id", hex.EncodeToString(decrypted.GetBucketId()))
		fmt.Println("metadata.project_bucket_prefix", hex.EncodeToString(decrypted.ProjectBucketPrefix))
		compact, err := metabase.ParseCompactBucketPrefix(decrypted.CompactProjectBucketPrefix)
		if err != nil {
			return err
		}
		fmt.Println("metadata.project_id", compact.ProjectID.String())
		fmt.Println("metadata.bucket_name", compact.BucketName)
		fmt.Println()
	}
	return nil
}
