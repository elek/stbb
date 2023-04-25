package satellite

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/storj"
)

type Restore struct {
	URL string `arg:""`
}

func (r Restore) restore() error {
	ctx := context.Background()

	ident, err := identity.FullIdentityFromPEM(Certificate, Key)
	if err != nil {
		return err
	}

	dialer, err := util.GetDialerForIdentity(ctx, ident, false, false)
	if err != nil {
		return err
	}
	nodeURL, err := storj.ParseNodeURL(r.URL)
	if err != nil {
		return err
	}
	conn, err := dialer.DialNodeURL(ctx, nodeURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewDRPCPiecestoreClient(util.NewTracedConnection(conn))
	restored, err := client.RestoreTrash(ctx, &pb.RestoreTrashRequest{})
	if err != nil {
		return err
	}
	fmt.Println(restored)
	return nil
}
