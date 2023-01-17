package piece

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs/v2"
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

func hashAlgo(s string) pb.PieceHashAlgorithm {
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

	//priv, err := hex.DecodeString("83603a2d16ddd8c38e838f353eb1560e60dfab9fdae71cf9b23f0ca4ad872757")
	//if err != nil {
	//	return
	//}
	pub, err := hex.DecodeString("9b322ffec5a8f5f769c31817b1bbfe788c19133a8613109eb2bed2d9f2e45862")
	if err != nil {
		return
	}
	noiseInfo := &pb.NoiseInfo{
		NoisePattern: pb.NoiseInfo_IK,
		Dh:           pb.NoiseInfo_DH25519,
		Cipher:       pb.NoiseInfo_CHACHA_POLY,
		Hash:         pb.NoiseInfo_BLAKE_2B,
		PublicKey:    pub,
	}
	d.conn, err = d.dialer.DialNodeURLWithNoise(ctx, d.storagenodeURL, noiseInfo)
	d.conn = experiment.NewConnWrapper(d.conn)
	d.client = pb.NewDRPCPiecestoreClient(d.conn)
	d.noSync = noSync
	d.hashAlgo = hashAlgo
	return
}

func (d DrpcUploader) Close() error {
	return d.conn.Close()
}

func (d DrpcUploader) Upload(ctx context.Context, file string) (uploaded int, err error) {
	if d.noSync {
		ctx = experiment.With(ctx, "nosync")
	}
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
		HashAlgorithm: d.hashAlgo,
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

	h := pb.NewHashFromAlgorithm(d.hashAlgo)

	source, err := os.Open(file)
	if err != nil {
		return 0, errs.Wrap(err)
	}

	buffer := make([]byte, 1024*1024*10)
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
			HashAlgorithm: d.hashAlgo,
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
		HashAlgorithm: d.hashAlgo,
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
