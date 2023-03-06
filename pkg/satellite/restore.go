package satellite

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/spf13/cobra"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/storj"
)

func init() {
	cmd := &cobra.Command{
		Use:   "restore <storagenode>",
		Short: "Send restore trash request to the storagenode",
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return restore(args[0])
	}
	SatelliteCmd.AddCommand(cmd)
}

func restore(url string) error {
	ctx := context.Background()

	ident, err := identity.FullIdentityFromPEM(cert, key)
	if err != nil {
		return err
	}

	dialer, err := util.GetDialerForIdentity(ctx, ident, false, false)
	if err != nil {
		return err
	}
	nodeURL, err := storj.ParseNodeURL(url)
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
