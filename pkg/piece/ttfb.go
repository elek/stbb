package piece

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs/v2"
	"os"
	"runtime/trace"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:   "ttfb <storagenode-id> <pieceid>",
		Short: "Download first bytes of a given piece",
		Args:  cobra.MinimumNArgs(2),
	}
	size := cmd.Flags().IntP("size", "", 256, "Number of bytes to download")
	useQuic := cmd.Flags().BoolP("quic", "q", false, "Force to use quic protocol")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {

		var output *os.File
		output, err := os.Create("trace.out")
		if err != nil {
			return errs.Wrap(err)
		}
		defer output.Close()

		err = trace.Start(output)
		if err != nil {
			return errs.Wrap(err)
		}
		defer trace.Stop()

		ctx := context.Background()
		start := time.Now()
		d, err := NewTTFBDownloader(ctx, args[0], *useQuic)
		if err != nil {
			return err
		}
		connected := time.Now()
		downloaded, err := d.Download(ctx, args[1], int64(*size))
		if err != nil {
			return err
		}
		fmt.Printf("Downloaded: %d, TTDial: %d ms, TTFB: %d ms\n", downloaded, connected.Sub(start).Milliseconds(), time.Since(start).Milliseconds())
		return nil
	}
	PieceCmd.AddCommand(cmd)
}

type TTFBDownloader struct {
	Downloader
	conn   *rpc.Conn
	client pb.DRPCPiecestoreClient
}

func NewTTFBDownloader(ctx context.Context, storagenodeURL string, useQuic bool) (d TTFBDownloader, err error) {
	d.Downloader, err = NewDownloader(ctx, storagenodeURL, useQuic, false)
	if err != nil {
		return
	}

	d.conn, err = d.dialer.DialNodeURL(ctx, d.storagenodeURL)
	if err != nil {
		return
	}
	d.client = pb.NewDRPCPiecestoreClient(d.conn)
	return
}

func (d TTFBDownloader) Close() error {
	return d.conn.Close()
}

func (d TTFBDownloader) Download(ctx context.Context, pieceToDownload string, size int64) (downloaded int, err error) {
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
			ChunkSize: int64(size),
		},
	})
	if err != nil {
		return
	}

	order := &pb.Order{
		SerialNumber: sn,
		Amount:       int64(size),
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

	downloaded = len(resp.Chunk.Data)
	return
}
