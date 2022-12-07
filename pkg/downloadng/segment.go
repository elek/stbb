package downloadng

import (
	"context"
	"storj.io/common/storj"
)

type SegmentDownloader struct {
	inbox       chan *DownloadPiece
	connections map[storj.NodeID]chan *DownloadPiece
	factory     func(url storj.NodeURL) (chan *DownloadPiece, error)
}

func NewSegmentDownloader(inbox chan *DownloadPiece, factory func(url storj.NodeURL) (chan *DownloadPiece, error)) *SegmentDownloader {
	return &SegmentDownloader{
		inbox:       inbox,
		connections: make(map[storj.NodeID]chan *DownloadPiece),
		factory:     factory,
	}
}

func (d *SegmentDownloader) Inbox() chan *DownloadPiece {
	return d.inbox
}

func (d *SegmentDownloader) Run(ctx context.Context) (err error) {
	defer func() {
		for _, c := range d.connections {
			close(c)
		}
	}()

	for {
		select {
		case req := <-d.inbox:
			if req == nil {
				return
			}
			worker, found := d.connections[req.orderLimit.StorageNodeId]
			if !found {
				ch, err := d.factory(storj.NodeURL{
					ID:      req.orderLimit.StorageNodeId,
					Address: req.sn.Address,
				})
				if err != nil {
					// we couldn't create anything which is connecting to this specific data
					// we should create and error responder worker
					panic(err)
				}
				d.connections[req.orderLimit.StorageNodeId] = ch
				worker = d.connections[req.orderLimit.StorageNodeId]
			}
			worker <- req
		case <-ctx.Done():
			return
		}

	}
}
