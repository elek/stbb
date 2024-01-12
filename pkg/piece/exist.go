package piece

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"path/filepath"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"time"
)

type Exist struct {
	util.DialerHelper
	NodeURL storj.NodeURL `arg:"" name:"nodeurl"`
	Piece   string        `arg:"" help:"Piece hash to download"`
	Keys    string        `help:"location of the identity files to sign orders"`
	Save    bool          `help:"safe piece to a file"`
}

func (d *Exist) Run() error {
	ctx, done := context.WithTimeout(context.Background(), 15*time.Second)
	defer done()

	satelliteIdentityCfg := identity.Config{
		CertPath: filepath.Join(d.Keys, "identity.cert"),
		KeyPath:  filepath.Join(d.Keys, "identity.key"),
	}
	ident, err := satelliteIdentityCfg.Load()
	if err != nil {
		panic(err)
	}

	dialer, err := util.GetDialerForIdentity(ctx, ident, false, false)
	if err != nil {
		return nil
	}
	conn, err := dialer.DialNodeURL(ctx, d.NodeURL)
	if err != nil {
		return nil
	}
	defer conn.Close()

	pieceID, err := storj.PieceIDFromString(d.Piece)
	if err != nil {
		return err
	}

	client := pb.NewDRPCPiecestoreClient(util.NewTracedConnection(conn))

	exists, err := client.Exists(ctx, &pb.ExistsRequest{
		PieceIds: []storj.PieceID{
			pieceID,
		},
	})
	if err != nil {
		return err
	}
	fmt.Println(exists.Missing)
	return nil
}
