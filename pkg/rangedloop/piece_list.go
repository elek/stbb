package rangedloop

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase/rangedloop"
	"time"
)

type PieceList struct {
	nodeIDs []storj.NodeID
	output  string
	index   int
}

type PieceListFork struct {
	writers map[storj.NodeID]*os.File
}

func NewPieceList(nodeIDs []storj.NodeID) *PieceList {
	return &PieceList{
		nodeIDs: nodeIDs,
	}
}

func (p *PieceList) Start(ctx context.Context, time time.Time) (err error) {
	return nil
}

func (p *PieceList) Fork(ctx context.Context) (rangedloop.Partial, error) {
	res := &PieceListFork{
		writers: make(map[storj.NodeID]*os.File),
	}
	for _, n := range p.nodeIDs {
		_ = os.MkdirAll(n.String(), 0755)
		outputFile := filepath.Join(n.String(), fmt.Sprintf("%s-%d", n, p.index))

		writer, err := os.Create(outputFile)
		if err != nil {
			return nil, err
		}
		res.writers[n] = writer
	}
	p.index++
	return res, nil

}

func (p *PieceList) Join(ctx context.Context, partial rangedloop.Partial) error {
	return partial.(*PieceListFork).Close()
}

func (p *PieceList) Finish(ctx context.Context) error {
	return nil
}

func (p *PieceListFork) Process(ctx context.Context, segments []rangedloop.Segment) error {
	for _, segment := range segments {
		for _, piece := range segment.Pieces {
			writer, found := p.writers[piece.StorageNode]
			if !found {
				continue
			}
			_, err := writer.WriteString(fmt.Sprintf("%s,%d,%d,%s,%s\n",
				segment.StreamID.String(),
				segment.Position.Encode(),
				piece.Number,
				segment.RootPieceID.String(),
				segment.RootPieceID.Derive(piece.StorageNode, int32(piece.Number)).String()))
			if err != nil {
				return errors.WithStack(err)
			}
		}

	}
	return nil
}

func (p *PieceListFork) Close() error {
	for _, n := range p.writers {
		err := n.Close()
		if err != nil {
			fmt.Println(err)
		}
	}
	return nil
}

var _ rangedloop.Observer = &PieceList{}
var _ rangedloop.Partial = &PieceListFork{}
