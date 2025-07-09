package segment

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"github.com/zeebo/blake3"
	"hash"
	"os"
	"path/filepath"
	"strings"
)

type Checksum struct {
	StreamID string `arg:""`
}

func (s *Checksum) Run() error {
	su, sp, err := util.ParseSegmentPosition(s.StreamID)
	if err != nil {
		return err
	}

	segmentDir := fmt.Sprintf("segment_%s_%d", su, sp.Encode())
	entries, err := os.ReadDir(segmentDir)
	if err != nil {
		return err
	}
	for _, e := range entries {

		if strings.Contains(e.Name(), ".") {
			name, algo, _ := strings.Cut(e.Name(), ".")
			var hasher hash.Hash
			switch algo {
			case "BLAKE3":
				hasher = blake3.New()
			case "SHA256":
				hasher = sha256.New()
			default:
				panic("Unsupported checksum algorithm: " + algo)
			}
			raw, err := os.ReadFile(filepath.Join(segmentDir, name))
			if err != nil {
				return errors.WithStack(err)
			}
			_, err = hasher.Write(raw)
			if err != nil {
				return errors.WithStack(err)
			}
			rawChecksum, err := os.ReadFile(filepath.Join(segmentDir, e.Name()))
			if err != nil {
				return errors.WithStack(err)
			}
			calculatedHash := hasher.Sum(nil)
			if !bytes.Equal(rawChecksum, calculatedHash) {
				fmt.Println(e.Name(), "Checksum mismatch", hex.EncodeToString(rawChecksum), hex.EncodeToString(calculatedHash))
			} else {
				fmt.Println(e.Name(), "Checksum OK")
			}

		}
	}
	return nil
}
