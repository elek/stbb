package crypto

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/elek/stbb/pkg/access"
	"github.com/pkg/errors"
	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/common/storj"
)

type EncryptPath struct {
	Bucket string `arg:""`
	Key    string `arg:""`
	Hash   bool   `help:"Also print out the hashed path (used by eventkit)"`
}

func (d EncryptPath) Run() error {
	access, err := access.ParseAccess(os.Getenv("UPLINK_ACCESS"))
	if err != nil {
		return err
	}

	path, err := encryption.EncryptPath(d.Bucket, paths.NewUnencrypted(d.Key), storj.EncAESGCM, access.EncAccess.Store)
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Println(hex.EncodeToString([]byte(path.Raw())))
	if d.Hash {
		fmt.Println(pathChecksum(path))
	}
	return nil
}

func pathChecksum(encPath paths.Encrypted) []byte {
	mac := hmac.New(sha1.New, []byte(encPath.Raw()))
	_, err := mac.Write([]byte("event"))
	if err != nil {
		panic(err)
	}
	return mac.Sum(nil)[:16]
}
