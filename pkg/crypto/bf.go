package crypto

import (
	"encoding/hex"
	"fmt"
	"github.com/zeebo/errs"
	"os"
	"storj.io/common/encryption"
	"storj.io/common/grant"
	"storj.io/common/paths"
	"storj.io/common/storj"
	"strings"
	"time"
)

type BruteForce struct {
	ProjectSalt  string `arg:""`
	BucketName   string `arg:""`
	EncryptedKey string `arg:""`
	PasswordFile string `arg:""`
}

func (d BruteForce) Run() error {
	rawSalt, err := hex.DecodeString(d.ProjectSalt)
	if err != nil {
		return errs.Wrap(err)
	}

	rawEncrypted, err := hex.DecodeString(d.EncryptedKey)
	if err != nil {
		return errs.Wrap(err)
	}

	passwords, err := os.ReadFile(d.PasswordFile)
	if err != nil {
		return errs.Wrap(err)
	}

	lastCheck := time.Now()
	for ix, line := range strings.Split(string(passwords), "\n") {
		if time.Since(lastCheck) > 1*time.Second {
			lastCheck = time.Now()
			fmt.Println(ix)
		}
		rootKey, err := encryption.DeriveRootKey([]byte(strings.TrimSpace(line)), rawSalt, "", 8)
		if err != nil {
			return errs.Wrap(err)
		}

		encAccess := grant.NewEncryptionAccessWithDefaultKey(rootKey)
		encAccess.SetDefaultPathCipher(storj.EncAESGCM)

		unEncrypted, err := encryption.DecryptPath(d.BucketName, paths.NewEncrypted(string(rawEncrypted)), storj.EncAESGCM, encAccess.Store)
		if err != nil {
			continue
		}
		fmt.Println(unEncrypted)
		break

	}
	return nil
}
