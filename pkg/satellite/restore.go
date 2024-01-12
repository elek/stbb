package satellite

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"path/filepath"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/storj"
)

type Restore struct {
	URL  string `arg:""`
	Keys string
}

func (r Restore) Run() error {
	ctx := context.Background()
	var err error
	var ident *identity.FullIdentity
	if r.Keys == "" {
		ident, err = identity.FullIdentityFromPEM(Certificate, Key)
	} else {
		satelliteIdentityCfg := identity.Config{
			CertPath: filepath.Join(r.Keys, "identity.cert"),
			KeyPath:  filepath.Join(r.Keys, "identity.key"),
		}
		ident, err = satelliteIdentityCfg.Load()
	}
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
	fmt.Println("called", restored)
	return nil
}
