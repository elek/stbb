package store

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"os"
	"storj.io/common/pb"
	"time"
)

type Header struct {
	File string `arg:"" help:"file to read"`
}

func (h Header) Run() error {
	sj1, err := os.ReadFile(h.File)
	if err != nil {
		return errors.WithStack(err)
	}
	l := binary.BigEndian.Uint16(sj1[0:2])
	fmt.Println(hex.EncodeToString(sj1[:l]))
	var ph pb.PieceHeader

	err = pb.Unmarshal(sj1[2:2+l], &ph)
	if err != nil {
		return errors.WithStack(err)
	}
	err = proto.MarshalText(os.Stdout, &ph)
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Println(ph.OrderLimit.PieceExpiration.Format(time.DateTime))
	return nil
}
