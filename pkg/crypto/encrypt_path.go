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

type EncryptPath struct {
	Bucket string `arg:""`
	Key    string `arg:""`
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
	return nil
}
