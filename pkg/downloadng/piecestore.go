package downloadng

import (
	"context"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"time"
)

type DownloadPiece struct {
	orderLimit *pb.OrderLimit
	pk         storj.PiecePrivateKey
	sn         *pb.NodeAddress
	size       int64
	buffer     []byte
	ecShare    int
	segmentID  storj.SegmentID
}

type DownloadSegment struct {
	ecShare   int
	segmentID storj.SegmentID

	// before download, we send a message with startTime and size.
	startTime time.Time
	size      int64
	cancel    func()

	// after download, we send one with the response.
	response *pb.PieceDownloadResponse
	sn       pb.NodeID
}

type PieceStoreClient struct {
	inbox  chan any
	outbox chan any
	dialer rpc.Dialer
	sn     storj.NodeURL
}

func NewPieceStoreClient(node storj.NodeURL, outbox chan any) (*PieceStoreClient, error) {
	dialer, err := getDialer(context.Background(), false)
	if err != nil {
		return nil, err
	}
	return &PieceStoreClient{
		inbox:  make(chan any),
		dialer: dialer,
		sn:     node,
		outbox: outbox,
	}, nil
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
			switch r := req.(type) {
			case *DownloadPiece:
				_, err = d.Download(ctx, client, r)
				// we need to send out errors and count them
				//fmt.Println(err)
			case FatalFailure:
				return
			case Done:
				return
			default:
				d.outbox <- d
			}

		case <-ctx.Done():
			return
		}

	}
}

func (d *PieceStoreClient) Download(ctx context.Context, client pb.DRPCPiecestoreClient, req *DownloadPiece) (downloaded int64, err error) {
	size := req.orderLimit.Limit

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	d.outbox <- &DownloadSegment{
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

		d.outbox <- &DownloadSegment{
			response:  resp,
			ecShare:   req.ecShare,
			segmentID: req.segmentID,
			sn:        req.orderLimit.StorageNodeId,
		}
		downloaded += int64(len(resp.Chunk.Data))
	}

	return
}
