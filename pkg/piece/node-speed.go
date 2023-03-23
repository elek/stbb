package piece

import (
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"storj.io/common/storj"
)

type NodeSpeed struct {
	util.DialerHelper
	Keys string `help:"location of the identity files to sign orders"`
	Path string `arg:"" help:"Key url (sj://bucket/encryptedpath)"`
}

func (n *NodeSpeed) Run() error {
	nodes := Nodes{
		DialerHelper: n.DialerHelper,
		Path:         n.Path,
		DesiredNodes: 200,
	}
	return nodes.OnEachNode(func(url storj.NodeURL, id storj.PieceID, size int64) error {
		d := DownloadDRPC{
			Keys: n.Keys,
			Loop: util.Loop{
				Verbose: false,
				Sample:  1,
			},
			DialerHelper: n.DialerHelper,
			NodeURL:      url,
			Piece:        id.String(),
			Size:         size,
		}
		err := d.Run()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println()
		return nil
	})
}
