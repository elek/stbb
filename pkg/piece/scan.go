package piece

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/spf13/cobra"
	"os"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"strings"
	"sync"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:  "scan <storagenode-address> <piecefile>",
		Args: cobra.ExactArgs(2),
	}
	dh := util.NewDialerHelper(cmd.Flags())
	w := cmd.Flags().IntP("worker", "w", 1, "Number of independent workers to use")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		start := time.Now()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		work := make(chan string)
		stat := make(chan int64)

		wg := sync.WaitGroup{}

		for i := 0; i < *w; i++ {
			go func() {
				for {
					select {
					case pieceID := <-work:
						start := time.Now()
						d, err := NewPieceScanner(ctx, args[0], dh)
						if err != nil {
							fmt.Println(err)
							return
						}
						_, err = d.Download(ctx, pieceID)
						wg.Done()
						if err != nil {
							fmt.Println(err)
						}
						d.Close()
						duration := time.Since(start).Milliseconds()
						stat <- duration
					case <-ctx.Done():
						return
					}
				}
			}()
		}
		go func() {
			counter := 0
			for {
				select {
				case duration, open := <-stat:
					if !open {
						return
					}
					counter++
					fmt.Println(duration)
				}
			}
		}()

		input, err := os.ReadFile(args[1])
		if err != nil {
			return err
		}
		rec := 0
		for _, line := range strings.Split(string(input), "\n") {
			parts := strings.Split(line, ",")
			if len(parts) < 4 {
				continue
			}
			wg.Add(1)
			work <- parts[2]
			rec++
			if rec > 3000 {
				break
			}
		}
		wg.Wait()
		close(stat)
		duration := time.Now().Sub(start).Milliseconds()
		fmt.Printf("%d pieces are checked during %d ms (%d req/sec)\n", rec, duration, rec*1000/int(duration))
		return nil
	}
	PieceCmd.AddCommand(cmd)
}

type PieceScanner struct {
	Downloader
	conn   *rpc.Conn
	client pb.DRPCPiecestoreClient
}

func NewPieceScanner(ctx context.Context, storagenodeURL string, dh *util.DialerHelper) (d PieceScanner, err error) {
	d.Downloader, err = NewDownloader(ctx, storagenodeURL, util.NewDialerHelper(nil))
	if err != nil {
		return
	}

	d.conn, err = dh.Connect(ctx, d.storagenodeURL)
	if err != nil {
		return
	}
	d.client = pb.NewDRPCPiecestoreClient(d.conn)
	return
}

func (d PieceScanner) Close() error {
	return d.conn.Close()
}

func (d PieceScanner) Download(ctx context.Context, pieceToDownload string) (downloaded int64, err error) {
	stream, err := d.client.Download(ctx)
	if err != nil {
		return
	}
	defer stream.Close()

	pieceID, err := storj.PieceIDFromString(pieceToDownload)
	if err != nil {
		return
	}

	size := int64(256)

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

	order := &pb.Order{
		SerialNumber: sn,
		Amount:       size,
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
	downloaded = int64(len(resp.Chunk.Data))
	return
}
