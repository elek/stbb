package downloadng

import (
	"context"
	"github.com/vivint/infectious"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"time"
)

type Parallel struct {
	global   chan any
	inbox    chan any
	outbox   chan any
	segments map[string]*segmentBuffer
}

func (p *Parallel) Add(req *DownloadSegment) *segmentBuffer {
	if _, found := p.segments[req.segmentID.String()]; !found {
		p.segments[req.segmentID.String()] = &segmentBuffer{
			results: map[storj.NodeID]*pieceBuffer{},
		}
	}
	return p.segments[req.segmentID.String()]
}

type pieceBuffer struct {
	start    time.Time
	duration time.Duration
	ecShare  int

	cancel       func()
	expectedSize int64
	size         int64
	chunks       []*pb.PieceDownloadResponse
}

func (b *pieceBuffer) Add(req *DownloadSegment) {
	if req.response != nil {
		b.chunks = append(b.chunks, req.response)
		b.size += int64(len(req.response.Chunk.Data))
		if b.size == b.expectedSize {
			b.duration = time.Since(b.start)
		}
	} else {
		b.expectedSize = req.size
		b.start = req.startTime
		b.cancel = req.cancel
		b.ecShare = req.ecShare
	}
}

func (b *pieceBuffer) HasStripe(offset int64, i int) bool {
	return b.size >= offset+int64(i)
}

type segmentBuffer struct {
	results         map[storj.NodeID]*pieceBuffer
	finished        bool
	duration        int64
	size            int64
	processedOffset int64
}

func (b *segmentBuffer) Add(req *DownloadSegment) {
	if _, found := b.results[req.sn]; !found {
		b.results[req.sn] = &pieceBuffer{
			chunks: []*pb.PieceDownloadResponse{},
		}
	}
	b.results[req.sn].Add(req)

	finished := 0

	// check if we have 29
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

func (b *segmentBuffer) ForwardDownloaded(outbox chan any) {
	// check if we can have at least 29 pieces from the next stripe
	for {
		pieces := make([]*pieceBuffer, 0)
		for _, piece := range b.results {
			if piece.HasStripe(b.processedOffset, 256) {
				pieces = append(pieces, piece)
			}
			if len(pieces) == 29 {
				// ready to decode next stripe

				c := &DecodeShares{
					shares: []infectious.Share{},
				}

				for _, p := range pieces {
					c.shares = append(c.shares, infectious.Share{
						Number: p.ecShare,
						// BIG TODO
						Data: p.chunks[0].Chunk.Data[b.processedOffset : b.processedOffset+256],
					})
				}
				outbox <- c
				b.processedOffset += int64(256)
				break
			}
		}
		if len(pieces) < 29 {
			break
		}
	}
}

func (p *Parallel) Run(ctx context.Context) error {
	done := false
	for {
		select {
		case req := <-p.inbox:
			if req == nil {
				return nil
			}

			switch r := req.(type) {
			case *DownloadSegment:
				segment := p.Add(r)
				segment.Add(r)

				segment.ForwardDownloaded(p.outbox)

				if segment.finished && !done {
					segment.Cancel()
					// todo: finish only if this is the last segment
					p.global <- Done{}
					done = true
				}
			case FatalFailure:
				p.outbox <- r
				return nil
			case Done:
				p.outbox <- r
				return nil
			default:
				p.outbox <- r
			}

		case <-ctx.Done():
			return nil
		}
	}
}
