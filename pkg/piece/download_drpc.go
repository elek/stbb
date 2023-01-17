package piece

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"strconv"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:  "download-drpc <storagenode-id> <pieceid> <size>",
		Args: cobra.ExactArgs(3),
	}
	samples := cmd.Flags().IntP("samples", "n", 1, "Number of tests to be executed")
	useQuic := cmd.Flags().BoolP("quic", "q", false, "Force to use quic protocol")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		start := time.Now()

		ctx := context.Background()

		downloadedBytes := int64(0)
		downloadedChunks := 0
		size, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		max := *samples
		for i := 0; i < max; i++ {
			d, err := NewDRPCDownloader(ctx, args[0], *useQuic)
			if err != nil {
				return err
			}
			n, c, err := d.Download(ctx, args[1], int64(size))
			if err != nil {
				return err
			}
			downloadedBytes += n
			downloadedChunks += c
			d.Close()
		}
		seconds := time.Now().Sub(start).Seconds()
		fmt.Printf("%d Mbytes are downloaded under %f sec (with %d chunk/RPC request in average), which is %f Mbytes/sec\n", downloadedBytes/1024/1024, seconds, downloadedChunks/max, float64(downloadedBytes)/seconds/1024/1024)
		return nil
	}
	PieceCmd.AddCommand(cmd)
}

type DRPCDownloader struct {
	Downloader
	conn   *rpc.Conn
	client pb.DRPCPiecestoreClient
}

func NewDRPCDownloader(ctx context.Context, storagenodeURL string, useQuic bool) (d DRPCDownloader, err error) {
	d.Downloader, err = NewDownloader(ctx, storagenodeURL, useQuic)
	if err != nil {
		return
	}
	noiseInfo := &pb.NoiseInfo{
		NoisePattern: pb.NoiseInfo_IK,
		Dh:           pb.NoiseInfo_DH25519,
		Cipher:       pb.NoiseInfo_CHACHA_POLY,
		Hash:         pb.NoiseInfo_BLAKE_2B,
	}
	d.conn, err = d.dialer.DialNodeURLWithNoise(ctx, d.storagenodeURL, noiseInfo)
	if err != nil {
		return
	}
	d.client = pb.NewDRPCPiecestoreClient(d.conn)
	return
}

func (d DRPCDownloader) Close() error {
	if d.conn != nil {
		return d.conn.Close()
	}
	return nil
}

func (d DRPCDownloader) Download(ctx context.Context, pieceToDownload string, size int64) (downloaded int64, chunks int, err error) {
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
	}

	return
}
