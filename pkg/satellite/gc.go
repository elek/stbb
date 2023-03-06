package satellite

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/spf13/cobra"
	"storj.io/common/bloomfilter"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:   "gc <storagenode>",
		Short: "Send gc request to the storagendoe",
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return gc(args[0])
	}
	SatelliteCmd.AddCommand(cmd)
}

func gc(url string) error {
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

	filter := bloomfilter.NewOptimal(10, 0.1)
	client := pb.NewDRPCPiecestoreClient(util.NewTracedConnection(conn))
	retain, err := client.Retain(ctx, &pb.RetainRequest{
		CreationDate: time.Now().Add(168 * time.Hour),
		Filter:       filter.Bytes(),
	})
	if err != nil {
		return err
	}
	fmt.Println(retain.String())
	return nil
}
