package bloom

import (
	"context"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"math"
	"storj.io/common/bloomfilter"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"time"
)

type Send struct {
	util.DialerHelper
	URL  storj.NodeURL
	Size int `default:"200000"`
}

func (s Send) Run() error {
	ctx := context.Background()

	dialer, err := s.CreateRPCDialer()
	conn, err := dialer.DialNodeURL(ctx, s.URL)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewDRPCPiecestoreClient(util.NewTracedConnection(conn))

	bf := make([]byte, s.Size)

	for i := 3; i < len(bf); i++ {
		bf[i] = 255
	}

	bitsPerElement := -1.44 * math.Log2(0.01)
	hashCountInt := int(math.Ceil(bitsPerElement * math.Log(2)))
	if hashCountInt > 32 {
		// it will never be larger, but just in case to avoid overflow
		hashCountInt = 32
	}

	seed := bloomfilter.GenerateSeed()

	bf[0] = 1
	bf[1] = byte(hashCountInt)
	bf[2] = seed

	req := &pb.RetainRequest{
		HashAlgorithm: pb.PieceHashAlgorithm_BLAKE3,
		CreationDate:  time.Now(),
		Filter:        bf,
	}

	_, err = client.Retain(ctx, req)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
