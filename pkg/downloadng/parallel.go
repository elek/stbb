package downloadng

import (
	"context"
	"fmt"
	"github.com/vivint/infectious"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"time"
)

type Parallel struct {
	inbox    chan *Download
	outbox   chan *DecodeShares
	segments map[string]*segmentBuffer
	finish   func()
}

func NewParallel(inbox chan *Download, downloader ObjectDownloader) Parallel {
	return Parallel{
		inbox:    inbox,
		outbox:   make(chan *DecodeShares),
		segments: map[string]*segmentBuffer{},
		finish: func() {
			close(downloader.inbox)
		},
	}
}

func (b *Parallel) Add(req *Download) *segmentBuffer {
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
	ecShare  int

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

func (b *segmentBuffer) Add(req *Download) {
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

func (b *segmentBuffer) ForwardDownloaded(outbox chan *DecodeShares) {
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

func (ec *Parallel) Run(ctx context.Context) error {
	for {
		select {
		case req := <-ec.inbox:
			if req == nil {
				return nil
			}

			segment := ec.Add(req)
			segment.Add(req)

			segment.ForwardDownloaded(ec.outbox)

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

func (ec *Parallel) Outbox() chan *DecodeShares {
	return ec.outbox
}
