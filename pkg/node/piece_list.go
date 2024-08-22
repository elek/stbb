package node

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"io"
	"os"
	"storj.io/storj/satellite/metabase"
	"time"
)

type PieceList struct {
	Aliases []int32 `arg:""`
	Limit   int64
	writers map[int32]io.WriteCloser
}

func (p *PieceList) Run() error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}
	ctx := context.TODO()

	metabaseDB, err := metabase.Open(ctx, log.Named("metabase"), os.Getenv("STBB_DB_METAINFO"), metabase.Config{
		ApplicationName: "stbb",
	})
	if err != nil {
		return errs.New("Error creating metabase connection: %+v", err)
	}
	defer func() {
		_ = metabaseDB.Close()
	}()

	p.writers = make(map[int32]io.WriteCloser)
	for _, alias := range p.Aliases {
		p.writers[alias], err = os.Create(fmt.Sprintf("%d.csv", alias))
		if err != nil {
			return err
		}
	}
	defer func() {
		for _, alias := range p.Aliases {
			if p.writers[alias] != nil {
				p.writers[alias].Close()
			}
		}
	}()

	progress := util.Progres{}
	err = metabaseDB.IterateLoopSegments(ctx, metabase.IterateLoopSegments{
		BatchSize:          100000,
		AsOfSystemInterval: 1 * time.Minute,
	}, func(ctx context.Context, iterator metabase.LoopSegmentsIterator) error {
		var entry metabase.LoopSegmentEntry
		for iterator.Next(ctx, &entry) {
			if entry.Inline() {
				continue
			}

			if p.Limit > 0 && progress.Counter() > p.Limit {
				return nil
			}
			err := p.check(entry)
			if err != nil {
				return err
			}
			progress.Increment()

		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (p *PieceList) check(entry metabase.LoopSegmentEntry) error {
	for _, alias := range p.Aliases {
		for _, pieceAlias := range entry.AliasPieces {
			if int32(pieceAlias.Alias) == alias {
				exp := int64(0)
				if entry.ExpiresAt != nil {
					exp = entry.ExpiresAt.Unix()
				}
				_, err := p.writers[alias].Write([]byte(fmt.Sprintf("%s,%d,%s,%d,%d,%d,%d,%d,%d\n", entry.StreamID, entry.Position.Encode(), entry.RootPieceID, pieceAlias.Number, entry.PlainSize, entry.EncryptedSize, entry.Placement, exp, entry.Redundancy.RequiredShares)))
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
