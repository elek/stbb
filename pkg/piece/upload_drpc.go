package piece

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/zeebo/errs/v2"
	"hash"
	"os"
	"storj.io/common/experiment"
	"storj.io/common/pb"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"strings"
)

type UploadDrpc struct {
	util.Loop
	util.DialerHelper
	NoSync  bool                  `help:"Disable file sync on upload"`
	Hash    pb.PieceHashAlgorithm `default:"0" help:"Piece hash algorithm to use"`
	NodeURL storj.NodeURL         `arg:"" name:"nodeurl"`
	File    string                `arg:"" help:"file to upload as a piece"`
	Keys    string                `help:"location of the identity files to sign orders"`
}

func (u *UploadDrpc) Run() error {
	ctx := context.Background()

	orderLimitCreator, err := util.NewKeySignerFromDir(u.Keys)
	if err != nil {
		return err
	}
	orderLimitCreator.Action = pb.PieceAction_PUT

	_, err = u.Loop.Run(func() error {
		_, _, err := u.ConnectAndUpload(ctx, orderLimitCreator)
		return err
	})
	return err
}

func (u *UploadDrpc) ConnectAndUpload(ctx context.Context, orderLimitCreator *util.KeySigner) (size int, id storj.PieceID, err error) {
	conn, err := u.Connect(ctx, u.NodeURL)
	if err != nil {
		return 0, id, err
	}
	defer conn.Close()

	//client := pb.NewDRPCReplaySafePiecestoreClient(conn)
	client := pb.NewDRPCPiecestoreClient(conn)

	size, id, err = u.Upload(ctx, client, orderLimitCreator)
	if err != nil {
		return size, id, err
	}
	if u.Verbose {
		fmt.Println("id:", id)
		fmt.Println("pieceHashAlgo:", u.Hash)

	}
	return size, id, nil
}

func (d *UploadDrpc) Upload(ctx context.Context, client pb.DRPCReplaySafePiecestoreClient, creator *util.KeySigner) (uploaded int, id storj.PieceID, err error) {
	defer mon.Task()(&ctx)(&err)
	pieceID := storj.NewPieceID()

	if d.NoSync {
		ctx = experiment.With(ctx, "nosync")
	}

	stream, err := client.Upload(ctx)
	if err != nil {
		return 0, pieceID, errs.Wrap(err)
	}
	defer stream.Close()

	stat, err := os.Stat(d.File)
	if err != nil {
		return 0, pieceID, errs.Wrap(err)
	}

	orderLimit, pk, serialNo, err := creator.CreateOrderLimit(ctx, pieceID, stat.Size(), creator.GetSatelliteID(), d.NodeURL.ID)
	if err != nil {
		return 0, pieceID, errs.Wrap(err)
	}

	err = stream.Send(&pb.PieceUploadRequest{
		Limit:         orderLimit,
		HashAlgorithm: d.Hash,
	})
	if err != nil {
		return 0, pieceID, errs.Wrap(err)
	}

	order := &pb.Order{
		SerialNumber: serialNo,
		Amount:       stat.Size(),
	}

	order, err = signing.SignUplinkOrder(ctx, pk, order)
	if err != nil {
		return 0, pieceID, errs.Wrap(err)
	}

	h := pb.NewHashFromAlgorithm(d.Hash)
	if d.Hash == pb.PieceHashAlgorithm(-1) {
		h = &NoHash{}
	}

	source, err := os.Open(d.File)
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
			HashAlgorithm: d.Hash,
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
		HashAlgorithm: d.Hash,
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
