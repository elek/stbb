package piece

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"storj.io/common/grant"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"strconv"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:  "piece-download <storagenode-id> <pieceid> <size>",
		Args: cobra.ExactArgs(3),
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		start := time.Now()

		ctx := context.Background()
		d, err := NewRawDownloader(ctx, args[0])
		if err != nil {
			return err
		}

		downloadedBytes := int64(0)
		downloadedChunks := 0
		size, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		max := 1000
		for i := 0; i < max; i++ {
			n, c, err := d.Download(ctx, args[1], int64(size))
			if err != nil {
				return err
			}
			downloadedBytes += n
			downloadedChunks += c
		}
		seconds := time.Now().Sub(start).Seconds()
		fmt.Printf("%d Mbytes are downloaded under %f sec (with %d chunk/RPC request in average), which is %f Mbytes/sec\n", downloadedBytes/1024/1024, seconds, downloadedChunks/max, float64(downloadedBytes)/seconds/1024/1024)
		return nil
	}
	PieceCmd.AddCommand(cmd)
}

type RawDownloader struct {
	satelliteURL   storj.NodeURL
	storagenodeURL storj.NodeURL
	conn           *rpc.Conn
	client         pb.DRPCPiecestoreClient
	fi             *identity.FullIdentity
	signee         signing.Signer
}

func NewRawDownloader(ctx context.Context, storagenodeURL string) (d RawDownloader, err error) {
	gr := os.Getenv("UPLINK_ACCESS")
	access, err := grant.ParseAccess(gr)
	if err != nil {
		return d, err
	}
	d.satelliteURL, err = storj.ParseNodeURL(access.SatelliteAddress)
	if err != nil {
		return
	}

	d.storagenodeURL, err = storj.ParseNodeURL(storagenodeURL)
	if err != nil {
		return
	}

	dialer, err := getDialer(ctx)
	if err != nil {
		return
	}

	d.conn, err = dialer.DialNodeURL(ctx, d.storagenodeURL)
	if err != nil {
		return
	}
	d.client = pb.NewDRPCPiecestoreClient(d.conn)

	satelliteIdentityCfg := identity.Config{
		CertPath: "identity.cert",
		KeyPath:  "identity.key",
	}
	d.fi, err = satelliteIdentityCfg.Load()
	if err != nil {
		return
	}

	d.signee = signing.SignerFromFullIdentity(d.fi)
	return
}

func (d RawDownloader) Close() error {
	return d.conn.Close()
}

func (d RawDownloader) Download(ctx context.Context, pieceToDownload string, size int64) (downloaded int64, chunks int, err error) {
	stream, err := d.client.Download(ctx)
	if err != nil {
		return
	}
	defer stream.Close()

	pub, priv, err := storj.NewPieceKey()
	if err != nil {
		return
	}

	pieceID, err := storj.PieceIDFromString(pieceToDownload)
	if err != nil {
		return
	}

	sn := storj.SerialNumber{}
	_, err = rand.Read(sn[:])
	if err != nil {
		return
	}

	orderLimit := &pb.OrderLimit{
		PieceId:         pieceID,
		SerialNumber:    sn,
		SatelliteId:     d.satelliteURL.ID,
		StorageNodeId:   d.storagenodeURL.ID,
		Action:          pb.PieceAction_GET,
		Limit:           size,
		OrderCreation:   time.Now(),
		OrderExpiration: time.Now().Add(24 * time.Hour),
		UplinkPublicKey: pub,
	}
	orderLimit, err = signing.SignOrderLimit(ctx, d.signee, orderLimit)
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
