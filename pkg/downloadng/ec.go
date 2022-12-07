package downloadng

import (
	"context"
	"fmt"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"time"
)

type ECDecoder struct {
	inbox    chan *Download
	segments map[string]*segmentBuffer
	finish   func()
}

func (b *ECDecoder) Add(req *Download) *segmentBuffer {
	if _, found := b.segments[req.segmentID.String()]; !found {
		b.segments[req.segmentID.String()] = &segmentBuffer{
			results: map[storj.NodeID]*pieceBuffer{},
		}
	}
	return b.segments[req.segmentID.String()]
}

type pieceBuffer struct {
	start    time.Time
	duration time.Duration

	cancel       func()
	expectedSize int64
	size         int64
	chunks       []*pb.PieceDownloadResponse
}

func (b *pieceBuffer) Add(req *Download) {
	if req.response != nil {
		b.chunks = append(b.chunks, req.response)
		b.size += int64(len(req.response.Chunk.Data))
		if b.size == b.expectedSize {
			b.duration = time.Since(b.start)
			fmt.Printf("Data #%d is downloaded %d %d during %d ms\n", req.ecShare, len(req.response.Chunk.Data), req.response.Chunk.Offset, b.duration.Milliseconds())
		}
	} else {
		b.expectedSize = req.size
		b.start = req.startTime
		b.cancel = req.cancel
	}
}

type segmentBuffer struct {
	results  map[storj.NodeID]*pieceBuffer
	finished bool
	duration int64
}

func (b *segmentBuffer) Add(req *Download) {
	if _, found := b.results[req.sn]; !found {
		b.results[req.sn] = &pieceBuffer{
			chunks: []*pb.PieceDownloadResponse{},
		}
	}
	b.results[req.sn].Add(req)

	finished := 0
	for _, piece := range b.results {
		if piece.expectedSize == piece.size {
			finished++
		}
	}
	if finished == 29 && !b.finished {
		b.finished = true

		for _, piece := range b.results {
			if b.duration == 0 || piece.duration.Milliseconds() > b.duration {
				b.duration = piece.duration.Milliseconds()
			}
		}

	}
}

func (b *segmentBuffer) Cancel() {
	for _, piece := range b.results {
		piece.cancel()
	}
}

func (ec *ECDecoder) Run(ctx context.Context) error {
	for {
		select {
		case req := <-ec.inbox:
			if req == nil {
				return nil
			}

			segment := ec.Add(req)
			segment.Add(req)
			if segment.finished {
				segment.Cancel()
				// todo: finish only if this is the last segment
				ec.finish()
				fmt.Printf("Segment is downloaded during %d ms using %d storagenode\n", segment.duration, len(segment.results))
				close(ec.inbox)
			}

		case <-ctx.Done():
			return nil
		}
	}
}
