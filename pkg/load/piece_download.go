package load

import (
	"context"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"io"
	"storj.io/common/memory"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"storj.io/uplink/private/piecestore"
)

type PieceDownload struct {
	util.DialerHelper
	util.WithKeySigner
	Runner
	NodeURL   storj.NodeURL
	PieceSize memory.Size `default:"1024"`
}

func (p *PieceDownload) Run() error {
	dialer, err := p.CreateRPCDialer()
	if err != nil {
		return errors.WithStack(err)
	}

	err = p.WithKeySigner.Init(pb.PieceAction_GET)
	if err != nil {
		return errors.WithStack(err)
	}

	p.RunTest(func(ctx context.Context, piece storj.PieceID) error {
		return p.connectAndDownload(ctx, dialer, piece)
	})
	return nil
}

func (p *PieceDownload) connectAndDownload(ctx context.Context, d rpc.Dialer, pieceID storj.PieceID) (err error) {
	client, err := piecestore.Dial(ctx, d, p.NodeURL, piecestore.DefaultConfig)
	if err != nil {
		return errors.WithStack(err)
	}

	defer func() {
		err = errs.Combine(err, client.Close())
	}()

	limit, privateKey, _, err := p.CreateOrderLimit(ctx, pieceID, p.PieceSize.Int64(), p.NodeURL.ID)
	download, err := client.Download(ctx, limit, privateKey, 0, p.PieceSize.Int64())
	if err != nil {
		return errs.Wrap(err)
	}
	defer func() {
		err = errs.Combine(err, download.Close())
	}()

	n, err := io.Copy(io.Discard, download)
	if err != nil {
		return errs.Wrap(err)
	}
	if n != p.PieceSize.Int64() {
		return errs.New("downloaded %d bytes, expected %d", n, p.PieceSize)
	}
	return nil
}
