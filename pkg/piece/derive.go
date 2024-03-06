package piece

import (
	"fmt"
	"storj.io/common/storj"
)

type Derive struct {
	NodeURL     storj.NodeURL `arg:""`
	RootPieceID storj.PieceID `arg:""`
	Index       int32         `arg:""`
}

func (d Derive) Run() error {
	pieceId := d.RootPieceID.Derive(d.NodeURL.ID, d.Index)
	fmt.Println(pieceId.String())
	return nil
}
