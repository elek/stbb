package piece

import (
	"bytes"
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"storj.io/common/storj"
	"storj.io/uplink/private/piecestore"
	"strconv"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:  "download-ps <storagenodeid> <piece-id> <size>",
		Args: cobra.ExactArgs(3),
	}
	samples := cmd.Flags().IntP("samples", "n", 1, "Number of tests to be executed")
	pooled := cmd.Flags().BoolP("pool", "p", false, "Use connection pool")
	quic := cmd.Flags().BoolP("quic", "q", false, "Force to use quic")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		start := time.Now()

		ctx := context.Background()
		d, err := NewPieceDownloader(ctx, args[0], *quic, *pooled)
		if err != nil {
			return err
		}
		defer d.Close()

		size, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}

		downloaded := int64(0)
		for i := 0; i < *samples; i++ {
			n, err := d.Download(ctx, args[1], int64(size))
			if err != nil {
				return err
			}
			downloaded += n
		}
		seconds := time.Now().Sub(start).Seconds()
		fmt.Printf("%d Mbytes are downloaded under %f sec, which is %f Mbytes/sec\n", downloaded/1024/1024, seconds, float64(downloaded)/seconds/1024/1024)
		return nil
	}
	PieceCmd.AddCommand(cmd)
}

type PieceDownloader struct {
	Downloader
	client *piecestore.Client
}

func NewPieceDownloader(ctx context.Context, storagenodeID string, useQuic bool, pooled bool) (PieceDownloader, error) {
	d, err := NewDownloader(ctx, storagenodeID, useQuic, pooled)
	if err != nil {
		return PieceDownloader{}, err
	}
	p := PieceDownloader{
		Downloader: d,
	}
	return p, nil

}

func (d PieceDownloader) Close() error {
	return nil
}

func (d PieceDownloader) Download(ctx context.Context, pieceId string, size int64) (downloaded int64, err error) {
	config := piecestore.DefaultConfig
	//config.DownloadBufferSize = 1024 * 1024
	//config.InitialStep = 1024 * 1024
	//config.MaximumStep = 1024 * 1024
	d.client, err = piecestore.Dial(ctx, d.dialer, d.storagenodeURL, config)
	if err != nil {
		return
	}
	defer d.client.Close()

	pieceID, err := storj.PieceIDFromString(pieceId)
	if err != nil {
		return
	}

	orderLimit, priv, _, err := d.OrderLimitCreator.CreateOrderLimit(ctx, pieceID, size, d.satelliteURL.ID, d.storagenodeURL.ID)
	if err != nil {
		return
	}

	download, err := d.client.Download(ctx, orderLimit, priv, 0, size)
	if err != nil {
		return
	}
	defer download.Close()
	buf := bytes.Buffer{}
	downloaded, err = io.Copy(&buf, download)
	if err != nil {
		return
	}
	return

}
