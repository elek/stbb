package downloadng

import (
	"context"
	"fmt"
	"github.com/vivint/infectious"
)

type ECDecoder struct {
	fc     *infectious.FEC
	inbox  chan *DecodeShares
	outbox chan *DecodeShares
}

type DecodeShares struct {
	shares []infectious.Share
}

type DecodedShare struct {
	encrypted []byte
}

func NewECDecoder(inbox chan *DecodeShares) (*ECDecoder, error) {
	fc, err := infectious.NewFEC(29, 119)
	if err != nil {
		return nil, err
	}
	return &ECDecoder{
		fc:    fc,
		inbox: inbox,
	}, nil
}

func (e *ECDecoder) Run(ctx context.Context) error {
	var dest []byte
	for {
		select {
		case req := <-e.inbox:
			if req == nil {
				return nil
			}
			decode, err := e.fc.Decode(dest, req.shares)
			if err != nil {
				//TODO: handle error?
				return err
			}
			fmt.Println(len(decode))
		case <-ctx.Done():
			return nil
		}

	}
}
