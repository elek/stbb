package util

import (
	"bytes"
	"context"
	"storj.io/common/pb"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"time"
)

type DownloadRequest struct {
	SatelliteID storj.NodeID
	PieceID     storj.PieceID
	Storagenode storj.NodeURL
	Size        int64
}

func DownloadPiece(ctx context.Context, client pb.DRPCReplaySafePiecestoreClient, creator *KeySigner, req DownloadRequest, handler func(data []byte, hash *pb.PieceHash, limit *pb.OrderLimit)) (downloaded int64, chunks int, err error) {
	defer mon.Task()(&ctx)(&err)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	stream, err := client.Download(ctx)
	if err != nil {
		return
	}
	defer stream.Close()

	orderLimit, priv, sn, err := creator.CreateOrderLimit(ctx, req.PieceID, req.Size, req.Storagenode.ID)
	if err != nil {
		return
	}

	var hash *pb.PieceHash
	var originalOrderLimit *pb.OrderLimit

	orderLimit.Action = pb.PieceAction_GET_REPAIR

	first := true

	chunkSize := req.Size
	requestSize := int64(req.Size)
	buff := bytes.NewBuffer([]byte{})
	for downloaded < req.Size {
		upperLimit := chunkSize + downloaded
		if upperLimit > req.Size {
			upperLimit = req.Size
		}

		order := &pb.Order{
			SerialNumber: sn,
			Amount:       upperLimit,
		}
		order, err = signing.SignUplinkOrder(ctx, priv, order)
		if err != nil {
			return
		}

		req := &pb.PieceDownloadRequest{
			Order: order,
		}
		if first {
			req.Limit = orderLimit
			req.Chunk = &pb.PieceDownloadRequest_Chunk{
				Offset:    0,
				ChunkSize: requestSize,
			}
		}
		first = false
		err = stream.Send(req)
		if err != nil {
			return
		}

		var resp *pb.PieceDownloadResponse
		resp, err = stream.Recv()
		if err != nil {
			return
		}
		if resp.Hash != nil {
			// GET_REPAIR sends the hash and the order limit first
			hash = resp.Hash
			originalOrderLimit = resp.Limit
			resp, err = stream.Recv()
			if err != nil {
				return
			}
		}

		chunks++
		downloaded += int64(len(resp.Chunk.Data))
		buff.Write(resp.Chunk.Data)
	}
	handler(buff.Bytes(), hash, originalOrderLimit)

	return
}
