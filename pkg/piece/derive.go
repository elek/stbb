package piece

import (
	"fmt"
	"storj.io/common/storj"
)

type Derive struct {
	NodeID      storj.NodeID  `arg:""`
	RootPieceID storj.PieceID `arg:""`
	Index       int32         `arg:""`
}

func (d Derive) Run() error {

	fmt.Println(d.RootPieceID.Derive(d.NodeID, d.Index))
	return nil
}
