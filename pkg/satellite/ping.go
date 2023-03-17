package satellite

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs"
	"os"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/storj"
)

func init() {
	cmd := &cobra.Command{
		Use:   "ping <storagenode>",
		Short: "Send ping to the storagenode",
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return ping(args[0])
	}
	SatelliteCmd.AddCommand(cmd)
}

func ping(url string) error {
	ctx := context.Background()

	cert, _ = os.ReadFile("identity.cert")
	key, _ = os.ReadFile("identity.key")
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
	conn, err := dialer.DialNode(ctx, nodeURL, rpc.DialOptions{
		ReplaySafe: true,
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewDRPCContactClient(util.NewTracedConnection(conn))
	pong, err := client.PingNode(ctx, &pb.ContactPingRequest{})
	if err != nil {
		return errs.Wrap(err)
	}
	fmt.Println(pong)
	return nil
}
