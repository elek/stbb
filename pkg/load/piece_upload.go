package load

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"io"
	"storj.io/common/memory"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"storj.io/uplink/private/piecestore"
	"time"
)

type PieceUpload struct {
	util.DialerHelper
	util.WithKeySigner
	Runner
	NodeURL   storj.NodeURL
	Slow      time.Duration
	PieceSize memory.Size `default:"1024"`
}

func (p *PieceUpload) Run() error {
	dialer, err := p.CreateRPCDialer()
	if err != nil {
		return errors.WithStack(err)
	}

	data, err := io.ReadAll(io.LimitReader(crand.Reader, p.PieceSize.Int64()))
	if err != nil {
		return errors.WithStack(err)
	}

	err = p.WithKeySigner.Init(pb.PieceAction_PUT)
	if err != nil {
		return errors.WithStack(err)
	}

	p.RunTest(func(ctx context.Context, piece storj.PieceID) error {
		return p.connectAndUpload(ctx, dialer, piece, data)
	})
	return nil
}

func (p *PieceUpload) connectAndUpload(ctx context.Context, d rpc.Dialer, pieceID storj.PieceID, data []byte) (err error) {
	client, err := piecestore.Dial(ctx, d, p.NodeURL, piecestore.DefaultConfig)
	if err != nil {
		return errors.WithStack(err)
	}
	client.UploadHashAlgo = pb.PieceHashAlgorithm(pb.PieceHashAlgorithm_BLAKE3)
	defer func() {
		_ = client.Close()
	}()

	limit, privateKey, _, err := p.CreateOrderLimit(ctx, pieceID, int64(len(data)), p.NodeURL.ID)
	_, err = client.UploadReader(ctx, limit, privateKey, SlowReader{
		pause:    p.Slow,
		original: bytes.NewReader(data),
	})
	return errors.WithStack(err)
}

type SlowReader struct {
	pause    time.Duration
	original io.Reader
}

func (s SlowReader) Read(p []byte) (n int, err error) {
	time.Sleep(s.pause)
	return s.original.Read(p)
}

var _ io.Reader = SlowReader{}
