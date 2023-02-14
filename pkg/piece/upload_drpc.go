package piece

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs/v2"
	"hash"
	"os"
	"storj.io/common/experiment"
	"storj.io/common/pb"
	"storj.io/common/rpc/rpcpool"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"strings"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:  "upload-drpc <storagenode-id> <file>",
		Args: cobra.ExactArgs(2),
	}
	samples := cmd.Flags().IntP("samples", "n", 1, "Number of tests to be executed")
	useQuic := cmd.Flags().BoolP("quic", "q", false, "Force to use quic protocol")
	pieceHashAlgo := cmd.Flags().StringP("hash", "", "SHA256", "Piece hash algorithm to use")
	noSync := cmd.Flags().BoolP("nosync", "", false, "Disable file sync on upload")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		start := time.Now()

		ctx := context.Background()

		uploaded := 0
		for i := 0; i < *samples; i++ {
			d, err := NewDRPCUploader(ctx, args[0], *useQuic, hashAlgo(*pieceHashAlgo), *noSync)
			if err != nil {
				return err
			}
			n, _, err := d.Upload(ctx, args[1])
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

func hashAlgo(s string) pb.PieceHashAlgorithm {
	if s == "NONE" {
		return pb.PieceHashAlgorithm(-1)
	}
	var available []string
	for value, name := range pb.PieceHashAlgorithm_name {
		available = append(available, name)
		if name == s {
			return pb.PieceHashAlgorithm(value)

		}
	}
	panic("Piece hash algorithm is invalid. Available options: " + strings.Join(available, ","))
}

type DrpcUploader struct {
	Downloader
	conn     rpcpool.Conn
	client   pb.DRPCPiecestoreClient
	hashAlgo pb.PieceHashAlgorithm
	noSync   bool
}

func NewDRPCUploader(ctx context.Context, storagenodeURL string, useQuic bool, hashAlgo pb.PieceHashAlgorithm, noSync bool) (d DrpcUploader, err error) {
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
	d.noSync = noSync
	d.hashAlgo = hashAlgo
	return
}

func (d DrpcUploader) Close() error {
	return d.conn.Close()
}

func (d DrpcUploader) Upload(ctx context.Context, file string) (uploaded int, id storj.PieceID, err error) {
	pieceID := storj.NewPieceID()
	if d.noSync {
		ctx = experiment.With(ctx, "nosync")
	}

	stream, err := d.client.Upload(ctx)
	if err != nil {
		return 0, pieceID, errs.Wrap(err)
	}
	defer stream.Close()

	stat, err := os.Stat(file)
	if err != nil {
		return 0, pieceID, errs.Wrap(err)
	}

	orderLimit, pk, sn, err := d.OrderLimitCreator.CreateOrderLimit(ctx, pieceID, stat.Size(), d.satelliteURL.ID, d.storagenodeURL.ID)
	if err != nil {
		return 0, pieceID, errs.Wrap(err)
	}

	err = stream.Send(&pb.PieceUploadRequest{
		Limit:         orderLimit,
		HashAlgorithm: d.hashAlgo,
	})
	if err != nil {
		return 0, pieceID, errs.Wrap(err)
	}

	order := &pb.Order{
		SerialNumber: sn,
		Amount:       stat.Size(),
	}

	order, err = signing.SignUplinkOrder(ctx, pk, order)
	if err != nil {
		return 0, pieceID, errs.Wrap(err)
	}

	h := pb.NewHashFromAlgorithm(d.hashAlgo)
	if d.hashAlgo == pb.PieceHashAlgorithm(-1) {
		h = &NoHash{}
	}

	source, err := os.Open(file)
	if err != nil {
		return 0, pieceID, errs.Wrap(err)
	}

	buffer := make([]byte, 1024*1024)
	written := 0
	for {
		n, err := source.Read(buffer)
		if err != nil {
			return 0, pieceID, errs.Wrap(err)
		}
		err = stream.Send(&pb.PieceUploadRequest{
			Order: order,
			Chunk: &pb.PieceUploadRequest_Chunk{
				Offset: int64(written),
				Data:   buffer[0:n],
			},
			HashAlgorithm: d.hashAlgo,
		})
		order = nil
		if err != nil {
			return 0, pieceID, errs.Wrap(err)
		}
		_, err = h.Write(buffer[0:n])
		if err != nil {
			return 0, pieceID, errs.Wrap(err)
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
		HashAlgorithm: d.hashAlgo,
	})

	err = stream.Send(&pb.PieceUploadRequest{
		Done: uplinkHash,
	})
	if err != nil {
		return 0, pieceID, errs.Wrap(err)
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return 0, pieceID, errs.Wrap(err)
	}
	return written, pieceID, nil
}

type NoHash struct {
}

func (n2 *NoHash) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (n2 *NoHash) Sum(b []byte) []byte {
	return []byte{1, 2, 3, 4}
}

func (n2 *NoHash) Reset() {

}

func (n2 *NoHash) Size() int {
	return 4
}

func (n2 *NoHash) BlockSize() int {
	return 4
}

var _ hash.Hash = &NoHash{}
