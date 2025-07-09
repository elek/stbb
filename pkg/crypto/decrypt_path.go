package crypto

import (
	"encoding/hex"
	"fmt"
	"github.com/elek/stbb/pkg/access"
	"github.com/pkg/errors"
	"os"
	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/common/storj"
)

type DecryptPath struct {
	Bucket string `arg:""`
	Key    string `arg:""`
}

func (d DecryptPath) Run() error {
	access, err := access.ParseAccess(os.Getenv("UPLINK_ACCESS"))
	if err != nil {
		return err
	}

	key, err := hex.DecodeString(d.Key)
	if err != nil {
		return errors.WithStack(err)
	}

	path, err := encryption.DecryptPath(d.Bucket, paths.NewEncrypted(string(key)), storj.EncAESGCM, access.EncAccess.Store)
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Println(path.Raw())
	return nil
}
