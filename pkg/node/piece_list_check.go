package node

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"storj.io/common/storj"
	"storj.io/storj/shared/tagsql"
	"storj.io/storj/storagenode/blobstore/filestore"
	"strconv"
	"time"
)

type PieceListCheck struct {
	File              string `arg:""`
	PieceExpirationDB string
	NodeID            storj.NodeID
	Verbose           bool
}

func (p *PieceListCheck) Run() error {
	report, err := os.Open(p.File)
	if err != nil {
		return errors.WithStack(err)
	}
	cr := csv.NewReader(report)
	stat := make(Stat)
	defer func() {
		stat.Summary()
	}()

	ctx := context.Background()

	sqlDB, err := tagsql.Open(ctx, "sqlite", "file:"+p.PieceExpirationDB+"?_busy_timeout=10000", nil)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		_ = sqlDB.Close()
	}()

	sqlDB.QueryContext(ctx, "SELECT ( ")

	progress := util.Progres{}
	for {
		line, err := cr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return errors.WithStack(err)
		}

		pieceID, err := storj.PieceIDFromString(line[2])
		if err != nil {
			return errors.WithStack(err)
		}

		pn, err := strconv.Atoi(line[3])
		if err != nil {
			return errors.WithStack(err)
		}

		derived := pieceID.Derive(p.NodeID, int32(pn))
		key := filestore.PathEncoding.EncodeToString(derived.Bytes())
		path := filepath.Join(key[:2], key[2:]+".sj1")
		var missing bool
		if _, err := os.Stat(path); err != nil {
			missing = true
		}

		expEpoch, err := strconv.Atoi(line[7])
		if err != nil {
			return errors.WithStack(err)
		}

		exp := time.Unix(int64(expEpoch), 0)
		if time.Now().After(exp) {
			stat["expired"]++
			if !missing {
				stat["expired_exists"]++
			}
		} else if missing {
			stat["missing"]++
		}
		stat["all"]++
		if p.Verbose {
			fmt.Println(line[0], line[1], path, exp)
		}
		progress.Increment()
	}
	return nil
}

type Stat map[string]int

func (s Stat) Increment(group string) {
	s[group]++
}

func (s Stat) Summary() {
	for k, v := range s {
		fmt.Println("  ", k, v)
	}
}
