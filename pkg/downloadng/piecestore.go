package downloadng

import (
	"context"
	"fmt"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"time"
)

type PieceStoreClient struct {
	inbox  chan *DownloadPiece
	outbox chan *Download
	dialer rpc.Dialer
	sn     storj.NodeURL
}

type DownloadPiece struct {
	orderLimit *pb.OrderLimit
	pk         storj.PiecePrivateKey
	sn         *pb.NodeAddress
	size       int64
	buffer     []byte
	ecShare    int
	segmentID  storj.SegmentID
}

type Download struct {
	ecShare   int
	segmentID storj.SegmentID

	// before download, we send a message with startTime and size,
	startTime time.Time
	size      int64
	cancel    func()

	//after download, we send one with the response
	response *pb.PieceDownloadResponse
	sn       pb.NodeID
}

func NewPieceStoreClient(node storj.NodeURL, outbox chan *Download) (*PieceStoreClient, error) {
	dialer, err := getDialer(context.Background(), true)
	if err != nil {
		return nil, err
	}
	return &PieceStoreClient{
		inbox:  make(chan *DownloadPiece),
		dialer: dialer,
		sn:     node,
		outbox: outbox,
	}, nil
}

func (d *PieceStoreClient) Inbox() chan *DownloadPiece {
	return d.inbox
}

func (d *PieceStoreClient) Run(ctx context.Context) {
	conn, err := d.dialer.DialNodeURL(ctx, d.sn)
	if err != nil {
		return
	}
	defer conn.Close()
	client := pb.NewDRPCPiecestoreClient(conn)

	for {
		select {
		case req := <-d.inbox:
			if req == nil {
				return
			}
			_, err = d.Download(ctx, client, req)
			fmt.Println(err)
		case <-ctx.Done():
			return
		}

	}
}

func (d *PieceStoreClient) Download(ctx context.Context, client pb.DRPCPiecestoreClient, req *DownloadPiece) (downloaded int64, err error) {
	size := req.orderLimit.Limit

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	d.outbox <- &Download{
		startTime: time.Now(),
		size:      size,
		ecShare:   req.ecShare,
		segmentID: req.segmentID,
		sn:        req.orderLimit.StorageNodeId,
		cancel:    cancel,
	}

	stream, err := client.Download(ctx)
	if err != nil {
		return
	}
	defer stream.Close()

	err = stream.Send(&pb.PieceDownloadRequest{
		Limit: req.orderLimit,
		Chunk: &pb.PieceDownloadRequest_Chunk{
			Offset:    0,
			ChunkSize: size,
		},
	})
	if err != nil {
		return
	}

	chunkSize := int64(size)
	for downloaded < size {
		upperLimit := chunkSize + downloaded
		if upperLimit > size {
			upperLimit = size
		}

		order := &pb.Order{
			SerialNumber: req.orderLimit.SerialNumber,
			Amount:       upperLimit,
		}
		order, err = signing.SignUplinkOrder(ctx, req.pk, order)
		if err != nil {
			return
		}

		err = stream.Send(&pb.PieceDownloadRequest{
			Order: order,
		})
		if err != nil {
			return
		}

		var resp *pb.PieceDownloadResponse
		resp, err = stream.Recv()
		if err != nil {
			return
		}

		d.outbox <- &Download{
			response:  resp,
			ecShare:   req.ecShare,
			segmentID: req.segmentID,
			sn:        req.orderLimit.StorageNodeId,
		}
	}

	return
}
