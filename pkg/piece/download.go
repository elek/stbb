package piece

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"storj.io/uplink/private/piecestore"
	"time"
)

func init() {
	PieceCmd.AddCommand(&cobra.Command{
		Use: "download",
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()

			ctx := context.Background()
			d, err := NewDownloadClient(ctx)
			if err != nil {
				return err
			}

			downloaded := int64(0)
			for i := 0; i < 100; i++ {
				n, err := d.Download(ctx)
				if err != nil {
					return err
				}
				downloaded += n
			}
			seconds := time.Now().Sub(start).Seconds()
			fmt.Printf("%d Mbytes are downloaded under %f sec, which is %f Mbytes/sec\n", downloaded/1024/1024, seconds, float64(downloaded)/seconds/1024/1024)
			return nil
		},
	})
}

type DownloadClient struct {
	satelliteURL   storj.NodeURL
	storagenodeURL storj.NodeURL
	fi             *identity.FullIdentity
	signee         signing.Signer
	client         *piecestore.Client
	dialer         rpc.Dialer
}

func NewDownloadClient(ctx context.Context) (d DownloadClient, err error) {
	d.satelliteURL, err = storj.ParseNodeURL("12whfK1EDvHJtajBiAUeajQLYcWqxcQmdYQU5zX5cCf6bAxfgu4@localhost:7777")
	if err != nil {
		return
	}

	d.storagenodeURL, err = storj.ParseNodeURL("12iRTsbfrsNjGLT55Qy8t6eNqnk2JcThrnPAk5R34bHsfTPT757@172.18.0.18:28967")
	if err != nil {
		return
	}

	d.dialer, err = getDialer(ctx)
	if err != nil {
		return
	}

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

func (d DownloadClient) Close() error {
	return d.Close()
}

func (d DownloadClient) Download(ctx context.Context) (downloaded int64, err error) {
	config := piecestore.DefaultConfig
	//config.DownloadBufferSize = 1024 * 1024
	//config.InitialStep = 1024 * 1024
	//config.MaximumStep = 1024 * 1024
	d.client, err = piecestore.Dial(ctx, d.dialer, d.storagenodeURL, config)
	if err != nil {
		return
	}
	defer d.client.Close()

	pub, priv, err := storj.NewPieceKey()
	if err != nil {
		return
	}

	pieceID, err := storj.PieceIDFromString("2VS3GZZZHFYIRRS2C6HVKNIS76VCMYEHOSACJ7HWDZGOMA4S7I3Q")
	if err != nil {
		return
	}

	sn := storj.SerialNumber{}
	_, err = rand.Read(sn[:])
	if err != nil {
		return
	}

	size := int64(5254400)
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
