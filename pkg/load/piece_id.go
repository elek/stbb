package load

import (
	"encoding/binary"
	"math/rand/v2"
	"storj.io/common/storj"
	"time"
)

type PieceIDStream struct {
	Seed      uint32
	generator *rand.ChaCha8
}

func (p *PieceIDStream) NextPieceID() storj.PieceID {
	if p.generator == nil {
		var seedBytes [32]byte
		if p.Seed == 0 {
			p.Seed = uint32(time.Now().UnixNano())
		}
		binary.BigEndian.PutUint32(seedBytes[:], p.Seed)
		p.generator = rand.NewChaCha8(seedBytes)
	}

	var id storj.PieceID
	_, err := p.generator.Read(id[:])
	if err != nil {
		panic(err)
	}

	return id
}
