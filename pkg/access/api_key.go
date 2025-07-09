package access

import (
	"encoding/hex"
	"fmt"
	"storj.io/common/base58"
	"storj.io/common/macaroon"
)

type ApiKey struct {
	Head   string `arg:""`
	Secret string `arg:""`
}

func (a ApiKey) Run() error {
	rawHead, err := hex.DecodeString(a.Head)
	if err != nil {
		return err
	}

	rawSecret, err := hex.DecodeString(a.Secret)
	if err != nil {
		return err
	}
	parts := macaroon.NewUnrestrictedFromParts(rawHead, rawSecret)
	raw := parts.Serialize()
	fmt.Print(base58.CheckEncode(raw, 0))
	return nil
}
