package crypto

import (
	"encoding/hex"
	"fmt"
	"github.com/zeebo/errs"
	"storj.io/common/encryption"
	"storj.io/common/grant"
	"storj.io/common/paths"
	"storj.io/common/storj"
)

type DecryptKey struct {
	ProjectSalt  string `arg:""`
	BucketName   string `arg:""`
	EncryptedKey string `arg:""`
	Password     string `arg:""`
}

func (d DecryptKey) Run() error {
	rawSalt, err := hex.DecodeString(d.ProjectSalt)
	if err != nil {
		return errs.Wrap(err)
	}

	rawEncrypted, err := hex.DecodeString(d.EncryptedKey)
	if err != nil {
		return errs.Wrap(err)
	}

	rootKey, err := encryption.DeriveRootKey([]byte(d.Password), rawSalt, "", 8)
	if err != nil {
		return errs.Wrap(err)
	}

	encAccess := grant.NewEncryptionAccessWithDefaultKey(rootKey)
	encAccess.SetDefaultPathCipher(storj.EncAESGCM)

	unEncrypted, err := encryption.DecryptPath(d.BucketName, paths.NewEncrypted(string(rawEncrypted)), storj.EncAESGCM, encAccess.Store)
	if err != nil {
		return errs.Wrap(err)
	}
	fmt.Println(unEncrypted)
	return nil
}
