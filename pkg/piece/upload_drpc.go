package piece

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
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
)

func init() {
	cmd := &cobra.Command{
		Use:  "upload-drpc <storagenode-id> <file>",
		Args: cobra.ExactArgs(2),
	}
	samples := cmd.Flags().IntP("samples", "n", 1, "Number of tests to be executed")
	pieceHashAlgo := cmd.Flags().StringP("hash", "", "SHA256", "Piece hash algorithm to use")
	noSync := cmd.Flags().BoolP("nosync", "", false, "Disable file sync on upload")
	verbose := cmd.Flags().BoolP("verbose", "v", false, "Verbose (print out piece hashes)")

	dh := util.NewDialerHelper(cmd.Flags())
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		_, err := util.Loop(*samples, *verbose, func() error {
			d, err := NewDRPCUploader(ctx, args[0], dh, hashAlgo(*pieceHashAlgo), *noSync)
			if err != nil {
				return err
			}
			_, h, err := d.Upload(ctx, args[1])
			if *verbose {
				fmt.Println("pieceHash:", h)
			}
			if err != nil {
				return err
			}
			d.Close()
			return nil
		})

		return err
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
	client   pb.DRPCReplaySafePiecestoreClient
	hashAlgo pb.PieceHashAlgorithm
	noSync   bool
}

func NewDRPCUploader(ctx context.Context, storagenodeURL string, dh *util.DialerHelper, hashAlgo pb.PieceHashAlgorithm, noSync bool) (d DrpcUploader, err error) {
	d.Downloader, err = NewDownloader(ctx, storagenodeURL, dh)
	if err != nil {
		return
	}
	d.OrderLimitCreator.(*KeySigner).action = pb.PieceAction_PUT

	d.conn, err = dh.Connect(ctx, d.storagenodeURL)
	if err != nil {
		return
	}
	d.client = pb.NewDRPCReplaySafePiecestoreClient(d.conn)
	d.noSync = noSync
	d.hashAlgo = hashAlgo
	return
}

func (d DrpcUploader) Close() error {
	return d.conn.Close()
}

func (d DrpcUploader) Upload(ctx context.Context, file string) (uploaded int, id storj.PieceID, err error) {
	defer mon.Task()(&ctx)(&err)
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
