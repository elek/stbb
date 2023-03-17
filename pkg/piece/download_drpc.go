package piece

import (
	"context"
	"github.com/elek/stbb/pkg/util"
	"github.com/spf13/cobra"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"strconv"
)

func init() {
	cmd := &cobra.Command{
		Use:  "download-drpc <storagenode-id> <pieceid> <size>",
		Args: cobra.ExactArgs(3),
	}
	samples := cmd.Flags().IntP("samples", "n", 1, "Number of tests to be executed")
	verbose := cmd.Flags().BoolP("verbose", "v", false, "Verbose")
	dh := util.NewDialerHelper(cmd.Flags())
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		downloadedBytes := int64(0)
		downloadedChunks := 0
		size, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		max := *samples
		_, err = util.Loop(max, *verbose, func() error {
			d, err := NewDRPCDownloader(ctx, args[0], dh)
			if err != nil {
				return err
			}
			n, c, err := d.Download(ctx, args[1], int64(size), func(bytes []byte) {})
			if err != nil {
				return err
			}
			downloadedBytes += n
			downloadedChunks += c
			d.Close()
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	}
	PieceCmd.AddCommand(cmd)
}

type DRPCDownloader struct {
	Downloader
	conn   *rpc.Conn
	client pb.DRPCReplaySafePiecestoreClient
}

func NewDRPCDownloader(ctx context.Context, storagenodeURL string, dh *util.DialerHelper) (d DRPCDownloader, err error) {
	d.Downloader, err = NewDownloader(ctx, storagenodeURL, dh)
	if err != nil {
		return
	}

	d.conn, err = dh.Connect(ctx, d.storagenodeURL)
	if err != nil {
		return
	}
	d.client = pb.NewDRPCReplaySafePiecestoreClient(util.NewTracedConnection(d.conn))
	return
}

func (d DRPCDownloader) Close() error {
	if d.conn != nil {
		return d.conn.Close()
	}
	return nil
}

func (d DRPCDownloader) Download(ctx context.Context, pieceToDownload string, size int64, handler func([]byte)) (downloaded int64, chunks int, err error) {
	defer mon.Task()(&ctx)(&err)
	stream, err := d.client.Download(ctx)
	if err != nil {
		return
	}
	defer stream.Close()

	pieceID, err := storj.PieceIDFromString(pieceToDownload)
	if err != nil {
		return
	}

	orderLimit, priv, sn, err := d.OrderLimitCreator.CreateOrderLimit(ctx, pieceID, size, d.satelliteURL.ID, d.storagenodeURL.ID)
	if err != nil {
		return
	}

	err = stream.Send(&pb.PieceDownloadRequest{
		Limit: orderLimit,
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
			SerialNumber: sn,
			Amount:       upperLimit,
		}
		order, err = signing.SignUplinkOrder(ctx, priv, order)
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
