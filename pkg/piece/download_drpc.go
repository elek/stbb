package piece

import (
	"context"
	"github.com/elek/stbb/pkg/util"
	"storj.io/common/pb"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"time"
)

type DownloadDRPC struct {
	util.Loop
	util.DialerHelper
	NodeURL storj.NodeURL `arg:"" name:"nodeurl"`
	Piece   string        `arg:"" help:"Piece hash to download"`
	Size    int64         `arg:"" help:"size of bytes to download"`
	Keys    string        `help:"location of the identity files to sign orders"`
}

func (d *DownloadDRPC) Run() error {
	orderLimitCreator, err := NewKeySignerFromDir(d.Keys)
	if err != nil {
		return err
	}
	orderLimitCreator.Action = pb.PieceAction_GET

	_, err = d.Loop.Run(func() error {

		ctx, done := context.WithTimeout(context.Background(), 15*time.Second)
		defer done()

		err = d.ConnectAndDownload(ctx, orderLimitCreator)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (d *DownloadDRPC) ConnectAndDownload(ctx context.Context, signer *KeySigner) error {
	conn, err := d.Connect(ctx, d.NodeURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewDRPCReplaySafePiecestoreClient(util.NewTracedConnection(conn))

	_, _, err = d.Download(ctx, client, signer, func(bytes []byte) {})
	return err
}
func (d *DownloadDRPC) Download(ctx context.Context, client pb.DRPCReplaySafePiecestoreClient, creator *KeySigner, handler func([]byte)) (downloaded int64, chunks int, err error) {
	defer mon.Task()(&ctx)(&err)
	stream, err := client.Download(ctx)
	if err != nil {
		return
	}
	defer stream.Close()

	pieceID, err := storj.PieceIDFromString(d.Piece)
	if err != nil {
		return
	}

	orderLimit, priv, sn, err := creator.CreateOrderLimit(ctx, pieceID, d.Size, creator.GetSatelliteID(), d.NodeURL.ID)
	if err != nil {
		return
	}

	first := true

	chunkSize := d.Size
	for downloaded < d.Size {
		upperLimit := chunkSize + downloaded
		if upperLimit > d.Size {
			upperLimit = d.Size
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
				ChunkSize: d.Size,
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
		//if chunkSize < 256*memory.KiB.Int64() {
		//	chunkSize = chunkSize * 3 / 2
		//	if chunkSize > 256*memory.KiB.Int64() {
		//		chunkSize = 256 * memory.KiB.Int64()
		//	}
		//}

		chunks++
		downloaded += int64(len(resp.Chunk.Data))
		handler(resp.Chunk.Data)
	}

	return
}
