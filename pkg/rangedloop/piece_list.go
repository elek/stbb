package rangedloop

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"os"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase/rangedloop"
	"time"
)

type PieceList struct {
	nodeID storj.NodeID
	output string
	index  int
}

type PieceListFork struct {
	NodeID storj.NodeID
	writer *os.File
}

func NewPieceList(nodeID storj.NodeID, output string) *PieceList {
	return &PieceList{
		nodeID: nodeID,
		output: output,
	}
}

func (p *PieceList) Start(ctx context.Context, time time.Time) (err error) {
	return nil
}

func (p *PieceList) Fork(ctx context.Context) (rangedloop.Partial, error) {
	outputFile := fmt.Sprintf("%s-%d", p.output, p.index)
	fmt.Println("Writing output to", outputFile)
	writer, err := os.Create(outputFile)
	if err != nil {
		return nil, err
	}
	p.index++
	return &PieceListFork{
		NodeID: p.nodeID,
		writer: writer,
	}, nil
}

func (p *PieceList) Join(ctx context.Context, partial rangedloop.Partial) error {
	return partial.(*PieceListFork).writer.Close()
}

func (p *PieceList) Finish(ctx context.Context) error {
	return nil
}

func (p *PieceListFork) Process(ctx context.Context, segments []rangedloop.Segment) error {
	for _, segment := range segments {
		for _, piece := range segment.Pieces {
			if piece.StorageNode == p.NodeID {
				_, err := p.writer.WriteString(fmt.Sprintf("%s,%d,%d,%s,%s\n",
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
	}
	return nil
}

var _ rangedloop.Observer = &PieceList{}
var _ rangedloop.Partial = &PieceListFork{}
