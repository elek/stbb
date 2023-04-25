package piece

import (
	"bytes"
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"io"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/piecestore"
	"time"
)

type DownloadPieceStore struct {
	util.Loop
	util.DialerHelper
	NodeURL storj.NodeURL `arg:"" name:"nodeurl"`
	Piece   string        `arg:"" help:"Piece hash to download"`
	Size    int64         `arg:"" help:"size of bytes to download"`
	Keys    string        `help:"location of the identity files to sign orders"`
}

func (d *DownloadPieceStore) Run() error {
	orderLimitCreator, err := NewKeySignerFromDir(d.Keys)
	if err != nil {
		return err
	}
	orderLimitCreator.Action = pb.PieceAction_GET

	_, err = d.Loop.Run(func() error {

		ctx, done := context.WithTimeout(context.Background(), 15*time.Second)
		defer done()

		_, err = d.Download(ctx, orderLimitCreator)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (d *DownloadPieceStore) Download(ctx context.Context, signer *KeySigner) (downloaded int64, err error) {
	config := piecestore.DefaultConfig
	//config.InitialStep = 64 * memory.KiB.Int64()
	//config.MaximumStep = 256 * memory.KiB.Int64()
	dialer, err := d.DialerHelper.CreateRPCDialer()
	if err != nil {
		return
	}
	client, err := piecestore.Dial(ctx, dialer, d.NodeURL, config)
	if err != nil {
		return
	}
	defer client.Close()

	pieceID, err := storj.PieceIDFromString(d.Piece)
	if err != nil {
		return
	}

	orderLimit, priv, _, err := signer.CreateOrderLimit(ctx, pieceID, d.Size, signer.GetSatelliteID(), d.NodeURL.ID)
	if err != nil {
		return
	}

	download, err := client.Download(ctx, orderLimit, priv, 0, d.Size)
	if err != nil {
		return
	}
	defer download.Close()
	buf := bytes.Buffer{}
	downloaded, err = io.Copy(&buf, download)
	if err != nil {
		return
	}
	if d.Verbose {
		fmt.Println("Downloaded", downloaded, "bytes")
	}
	return

}
