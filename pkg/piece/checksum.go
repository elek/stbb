package piece

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/zeebo/blake3"
)

type Checksum struct {
	File string `arg:"" help:"checksum file to be used"`
}

func (c *Checksum) Run() error {
	raw, err := os.ReadFile(c.File)
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Println("Checksum:", hex.EncodeToString(raw))
	var hasher hash.Hash
	switch strings.ToLower(filepath.Ext(c.File)) {
	case ".blake3":
		hasher = blake3.New()
	case ".sha256":
		hasher = sha256.New()
	default:
		return errors.New("unknown checksum file extension: " + filepath.Ext(c.File))
	}
	data, err := os.ReadFile(strings.TrimSuffix(c.File, filepath.Ext(c.File)))
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = hasher.Write(data)
	if err != nil {
		return errors.WithStack(err)
	}

	matched := -1
	for i := 0; i < 1024; i++ {
		calculatedHash := hasher.Sum(nil)
		if bytes.Equal(raw, calculatedHash) {
			matched = i
			break
		}
		_, err := hasher.Write([]byte{0})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	if matched >= 0 {
		fmt.Println("Checksum OK", matched)
	} else {
		fmt.Println("Checksum FAILED")
	}
	return nil
}
