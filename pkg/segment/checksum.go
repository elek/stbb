package segment

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/elek/stbb/pkg/db"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"github.com/zeebo/blake3"
	"go.uber.org/zap"
	"storj.io/storj/satellite/metabase"
)

type Checksum struct {
	db.WithDatabase
	StreamID  string `arg:""`
	PieceInfo bool   `help:"Print additional information (with using db / segment list) "`
}

func (s *Checksum) Run() error {
	su, sp, err := util.ParseSegmentPosition(s.StreamID)
	if err != nil {
		return err
	}

	ctx := context.Background()
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	var segment metabase.Segment
	if s.PieceInfo {
		metabaseDB, err := s.WithDatabase.GetMetabaseDB(ctx, log)
		if err != nil {
			return errors.WithStack(err)
		}
		defer metabaseDB.Close()

		su, sp, err := util.ParseSegmentPosition(s.StreamID)
		if err != nil {
			return err
		}

		segment, err = metabaseDB.GetSegmentByPosition(ctx, metabase.GetSegmentByPosition{
			StreamID: su,
			Position: sp,
		})
	}

	segmentDir := fmt.Sprintf("segment_%s_%d", su, sp.Encode())
	entries, err := os.ReadDir(segmentDir)
	if err != nil {
		return err
	}
	var files []string
	for _, e := range entries {
		files = append(files, e.Name())
	}
	ix := func(s string) int {
		parts := strings.Split(s, "_")
		if len(parts) < 2 {
			return -1
		}
		numStr := parts[0]
		num, _ := strconv.Atoi(numStr)
		return num
	}
	sort.Slice(files, func(i, j int) bool {
		return ix(files[i]) < ix(files[j])
	})
	for _, f := range files {
		if strings.Contains(f, ".") {
			name, algo, _ := strings.Cut(f, ".")
			var hasher hash.Hash
			switch algo {
			case "BLAKE3":
				hasher = blake3.New()
			case "SHA256":
				hasher = sha256.New()
			case "hash", "orderlimit":
				continue
			default:
				panic("Unsupported checksum algorithm: " + algo + " in file " + f)
			}
			raw, err := os.ReadFile(filepath.Join(segmentDir, name))
			if err != nil {
				return errors.WithStack(err)
			}
			_, err = hasher.Write(raw)
			if err != nil {
				return errors.WithStack(err)
			}
			rawChecksum, err := os.ReadFile(filepath.Join(segmentDir, f))
			if err != nil {
				return errors.WithStack(err)
			}
			calculatedHash := hasher.Sum(nil)
			if !bytes.Equal(rawChecksum, calculatedHash) {
				if !s.PieceInfo {
					fmt.Println(f, "Checksum mismatch", hex.EncodeToString(rawChecksum), hex.EncodeToString(calculatedHash))
				} else {
					position, rest, _ := strings.Cut(f, "_")
					pieceID, checksum, _ := strings.Cut(rest, ".")
					number, err := strconv.Atoi(position)
					if err != nil {
						return errors.WithStack(err)
					}

					piece, _ := segment.Pieces.FindByNum(number)

					fmt.Println(f, "Checksum mismatch", hex.EncodeToString(rawChecksum), hex.EncodeToString(calculatedHash), position, pieceID, checksum, piece.StorageNode, piece.Number)

				}
			} else {
				if !s.PieceInfo {
					fmt.Println(f, "Checksum OK")
				}
			}

		}
	}
	return nil
}
