package piece

import (
	"bytes"
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"os"
	"storj.io/common/pb"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"strconv"
	"strings"
)

type Audit struct {
	util.DialerHelper
	Piece   string
	NodeURL storj.NodeURL
	buffer  *bytes.Buffer
	Keys    string   `help:"location of the identity files to sign orders"`
	Args    []string `arg:""`
}

func (a Audit) Run() error {
	orderLimitCreator, err := util.NewKeySignerFromDir(a.Keys)
	if err != nil {
		return err
	}
	a.buffer = bytes.NewBuffer(make([]byte, 3000000))
	orderLimitCreator.Action = pb.PieceAction_GET_REPAIR

	ctx := context.Background()

	conn, err := a.Connect(ctx, a.NodeURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewDRPCReplaySafePiecestoreClient(conn)

	var pieceID storj.PieceID
	var size int64

	switch len(a.Args) {
	case 3:
		rootPieceID, err := storj.PieceIDFromString(a.Args[0])
		if err != nil {
			return err
		}
		pieceNum, err := strconv.Atoi(a.Args[1])
		if err != nil {
			return err
		}

		pieceID = rootPieceID.Derive(a.NodeURL.ID, int32(pieceNum))
		s, err := strconv.Atoi(a.Args[2])
		if err != nil {
			return err
		}
		size = int64(s)/29 + 256

		err = a.check(ctx, client, orderLimitCreator, pieceID, size)
		if err != nil {
			return err
		}
	case 2:
		pieceID, err = storj.PieceIDFromString(a.Args[0])
		if err != nil {
			return err
		}
		s, err := strconv.Atoi(a.Args[1])
		if err != nil {
			return err
		}
		size = int64(s)
		err = a.check(ctx, client, orderLimitCreator, pieceID, size)
		if err != nil {
			return err
		}
	case 1:
		raw, err := os.ReadFile(a.Args[0])
		if err != nil {
			return errors.WithStack(err)
		}
		for _, line := range strings.Split(string(raw), "\n") {
			line = strings.TrimSpace(line)
			parts := strings.Split(line, ",")
			if len(parts) < 3 {
				continue
			}

			rootPieceID, err := storj.PieceIDFromString(parts[0])
			if err != nil {
				return err
			}
			pieceNum, err := strconv.Atoi(parts[1])
			if err != nil {
				return err
			}

			pieceID = rootPieceID.Derive(a.NodeURL.ID, int32(pieceNum))
			s, err := strconv.Atoi(parts[3])
			if err != nil {
				return err
			}
			size = int64(s)/29 + 256
			err = a.check(ctx, client, orderLimitCreator, pieceID, size)
			if err != nil {
				fmt.Println(line, err.Error())
			} else {
				fmt.Println(line)
			}
		}

	default:
		panic("wrong number of arguments")
	}

	if len(a.Args) == 2 {

	}

	return nil

}

func (a Audit) check(ctx context.Context, client pb.DRPCReplaySafePiecestoreClient, creator *util.KeySigner, id storj.PieceID, size int64) error {
	stream, err := client.Download(ctx)
	if err != nil {
		return err
	}
	orderLimit, priv, sn, err := creator.CreateOrderLimit(ctx, id, int64(size), a.NodeURL.ID)
	if err != nil {
		return nil
	}

	first := true

	chunkSize := size
	var downloaded int64

	a.buffer.Reset()
	var hash *pb.PieceHash
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
			return err
		}

		req := &pb.PieceDownloadRequest{
			Order: order,
		}
		if first {
			req.Limit = orderLimit
			req.Chunk = &pb.PieceDownloadRequest_Chunk{
				Offset:    0,
				ChunkSize: size,
			}
		}
		first = false
		err = stream.Send(req)
		if err != nil {
			return err
		}

		var resp *pb.PieceDownloadResponse
		resp, err = stream.Recv()
		if err != nil {
			return err
		}
		if resp.Hash != nil {
			hash = resp.Hash
		}
		if resp.Chunk == nil {
			continue
		}

		downloaded += int64(len(resp.Chunk.Data))
		a.buffer.Write(resp.Chunk.Data)

	}
	h := pb.NewHashFromAlgorithm(hash.HashAlgorithm)
	_, err = h.Write(a.buffer.Bytes())
	if err != nil {
		return err
	}
	if !bytes.Equal(h.Sum(nil), hash.Hash) {
		return errors.New("hash mismatch")
	}
	return nil
}
