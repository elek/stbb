package downloadng

import (
	"context"
	"github.com/vivint/infectious"
)

type ECDecoder struct {
	fc     *infectious.FEC
	inbox  chan any
	outbox chan any
}

type DecodeShares struct {
	shares []infectious.Share
}

type DecodedShare struct {
	encrypted []byte
}

func NewECDecoder(inbox chan any) (*ECDecoder, error) {
	fc, err := infectious.NewFEC(29, 119)
	if err != nil {
		return nil, err
	}
	return &ECDecoder{
		fc:     fc,
		inbox:  logReceived("ECDecoder", inbox),
		outbox: make(chan any),
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
			switch r := req.(type) {
			case *DecodeShares:
				decoded, err := e.fc.Decode(dest, r.shares)
				if err != nil {
					//TODO: handle error?
					return err

				}
				e.outbox <- &DecryptBuffer{
					encrypted: decoded,
				}
			case Done:
				e.outbox <- r
				return nil
			default:
				e.outbox <- r
			}

		case <-ctx.Done():
			return nil
		}

	}
}
