package piece

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/elek/stbb/pkg/util"
	"storj.io/common/pb"
	"storj.io/common/storj"
)

type DownloadDRPC struct {
	util.Loop
	util.DialerHelper
	NodeURL storj.NodeURL `arg:"" name:"nodeurl"`
	Piece   storj.PieceID `arg:"" help:"Piece hash to download"`
	Size    int64         `arg:"" help:"size of bytes to download"`
	Keys    string        `help:"location of the identity files to sign orders"`
	Save    bool          `help:"safe piece to a file"`
}

func (d *DownloadDRPC) Run() error {
	orderLimitCreator, err := util.NewKeySignerFromDir(d.Keys)
	if err != nil {
		return err
	}
	orderLimitCreator.Action = pb.PieceAction_GET_REPAIR

	_, err = d.Loop.Run(func() error {

		ctx, done := context.WithTimeout(context.Background(), 15*time.Second)
		defer done()

		err = d.ConnectAndDownload(ctx, orderLimitCreator)
		if d.Verbose {
			if err != nil {
				fmt.Println(d.NodeURL.String() + "," + d.Piece.String() + "," + err.Error())
			} else {
				fmt.Println(d.NodeURL.String() + "," + d.Piece.String())
			}

		}
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (d *DownloadDRPC) ConnectAndDownload(ctx context.Context, signer *util.KeySigner) error {
	conn, err := d.Connect(ctx, d.NodeURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewDRPCReplaySafePiecestoreClient(util.NewTracedConnection(conn))

	_, _, err = util.DownloadPiece(ctx, client, signer, util.DownloadRequest{
		SatelliteID: signer.GetSatelliteID(),
		Storagenode: d.NodeURL,
		PieceID:     d.Piece,
		Size:        d.Size,
	}, func(data []byte, hash *pb.PieceHash, limit *pb.OrderLimit) {
		if hash != nil {
			fmt.Println(hash.HashAlgorithm.String(), hex.EncodeToString(hash.Hash))
		}
		if d.Save {
			err := os.WriteFile(d.Piece.String(), data, 0644)
			if err != nil {
				panic(err)
			}
			if hash != nil {
				marshalled, err := pb.Marshal(hash)
				if err != nil {
					panic(err)
				}
				err = os.WriteFile(d.Piece.String()+".hash", marshalled, 0644)
			}
			if limit != nil {
				marshalled, err := pb.Marshal(limit)
				if err != nil {
					panic(err)
				}
				err = os.WriteFile(d.Piece.String()+".orderlimit", marshalled, 0644)
			}
			if err != nil {
				panic(err)
			}

		}

	})
	return err
}
