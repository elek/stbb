package downloadng

import (
	"context"
	"storj.io/common/storj"
)

type DownloadRouter struct {
	inbox       chan any
	outbox      chan any
	connections map[storj.NodeID]chan any
	factory     func(url storj.NodeURL, outbox chan any) (chan any, error)
}

func (d *DownloadRouter) Run(ctx context.Context) (err error) {
	defer close(d.outbox)

	for {
		select {
		case req := <-d.inbox:
			if req == nil {
				return
			}
			switch r := req.(type) {
			case *DownloadPiece:
				err := d.pieceDownloader(r)
				if err != nil {
					// not a problem if we have enough connections. Probably count it.
				}
			case FatalFailure:
				for _, c := range d.connections {
					c <- r
				}
				d.outbox <- r
				return nil
			case Done:
				for _, c := range d.connections {
					c <- r
				}
				d.outbox <- r
				return nil
			default:
				d.outbox <- r
			}

		case <-ctx.Done():
			return
		}

	}
}

func (d *DownloadRouter) pieceDownloader(req *DownloadPiece) error {
	worker, found := d.connections[req.orderLimit.StorageNodeId]
	if !found {
		ch, err := d.factory(storj.NodeURL{
			ID:      req.orderLimit.StorageNodeId,
			Address: req.sn.Address,
		}, d.outbox)
		if err != nil {
			return err
		}
		d.connections[req.orderLimit.StorageNodeId] = ch
		worker = d.connections[req.orderLimit.StorageNodeId]
	}
	worker <- req
	return nil
}
