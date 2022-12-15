package piece

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs/v2"
	"os"
	"storj.io/common/pb"
	"storj.io/common/pkcrypto"
	"storj.io/common/rpc"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:  "upload-drpc <storagenode-id> <file>",
		Args: cobra.ExactArgs(2),
	}
	samples := cmd.Flags().IntP("samples", "n", 1, "Number of tests to be executed")
	useQuic := cmd.Flags().BoolP("quic", "q", false, "Force to use quic protocol")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		start := time.Now()

		ctx := context.Background()

		uploaded := 0
		for i := 0; i < *samples; i++ {
			d, err := NewDRPCUploader(ctx, args[0], *useQuic)
			if err != nil {
				return err
			}
			n, err := d.Upload(ctx, args[1])
			if err != nil {
				return err
			}
			uploaded += n
			d.Close()
		}

		seconds := time.Now().Sub(start).Seconds()
		fmt.Printf("%d Mbytes are uploaded under %f sec, which is %f Mbytes/sec\n", uploaded/1024/1024, seconds, float64(uploaded)/seconds/1024/1024)
		return nil
	}
	PieceCmd.AddCommand(cmd)
}

type DrpcUploader struct {
	Downloader
	conn   *rpc.Conn
	client pb.DRPCPiecestoreClient
}

func NewDRPCUploader(ctx context.Context, storagenodeURL string, useQuic bool) (d DrpcUploader, err error) {
	d.Downloader, err = NewDownloader(ctx, storagenodeURL, useQuic)
	if err != nil {
		return
	}
	d.OrderLimitCreator.(*KeySigner).action = pb.PieceAction_PUT
	d.conn, err = d.dialer.DialNodeURL(ctx, d.storagenodeURL)
	if err != nil {
		return
	}
	d.client = pb.NewDRPCPiecestoreClient(d.conn)
	return
}

func (d DrpcUploader) Close() error {
	return d.conn.Close()
}

func (d DrpcUploader) Upload(ctx context.Context, file string) (uploaded int, err error) {
	stream, err := d.client.Upload(ctx)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	defer stream.Close()

	pieceID := storj.NewPieceID()

	stat, err := os.Stat(file)
	if err != nil {
		return 0, errs.Wrap(err)
	}

	orderLimit, pk, sn, err := d.OrderLimitCreator.CreateOrderLimit(ctx, pieceID, stat.Size(), d.satelliteURL.ID, d.storagenodeURL.ID)
	if err != nil {
		return 0, errs.Wrap(err)
	}

	err = stream.Send(&pb.PieceUploadRequest{
		Limit:         orderLimit,
		HashAlgorithm: pb.PieceHashAlgorithm_SHA256,
	})
	if err != nil {
		return 0, errs.Wrap(err)
	}

	order := &pb.Order{
		SerialNumber: sn,
		Amount:       stat.Size(),
	}

	order, err = signing.SignUplinkOrder(ctx, pk, order)
	if err != nil {
		return 0, errs.Wrap(err)
	}

	h := pkcrypto.NewHash()

	source, err := os.Open(file)
	if err != nil {
		return 0, errs.Wrap(err)
	}

	buffer := make([]byte, 1024*1024)
	written := 0
	for {
		n, err := source.Read(buffer)
		if err != nil {
			return 0, errs.Wrap(err)
		}
		err = stream.Send(&pb.PieceUploadRequest{
			Order: order,
			Chunk: &pb.PieceUploadRequest_Chunk{
				Offset: int64(written),
				Data:   buffer[0:n],
			},
			HashAlgorithm: pb.PieceHashAlgorithm_SHA256,
		})
		order = nil
		if err != nil {
			return 0, errs.Wrap(err)
		}
		_, err = h.Write(buffer[0:n])
		if err != nil {
			return 0, errs.Wrap(err)
		}

		written += n
		if written >= int(stat.Size()) {
			break
		}
	}

	uplinkHash, err := signing.SignUplinkPieceHash(ctx, pk, &pb.PieceHash{
		PieceId:       pieceID,
		PieceSize:     stat.Size(),
		Hash:          h.Sum(nil),
		Timestamp:     orderLimit.OrderCreation,
		HashAlgorithm: pb.PieceHashAlgorithm_SHA256,
	})

	err = stream.Send(&pb.PieceUploadRequest{
		Done: uplinkHash,
	})
	if err != nil {
		return 0, errs.Wrap(err)
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return 0, errs.Wrap(err)
	}
	return written, nil
}
