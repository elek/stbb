package rpc

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs/v2"
)

func init() {
	cmd := &cobra.Command{
		Use:   "space <dir>",
		Short: "Free space",
		Args:  cobra.ExactArgs(1),
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		blobs, err := createBlobs(args[0])
		if err != nil {
			return err
		}
		space, err := blobs.FreeSpace(ctx)
		if err != nil {
			return errs.Wrap(err)
		}
		fmt.Println("Free space", space)

		forBlobs, err := blobs.SpaceUsedForBlobs(ctx)
		if err != nil {
			return errs.Wrap(err)
		}
		fmt.Println("For blobs", forBlobs)

		forTrash, err := blobs.SpaceUsedForTrash(ctx)
		if err != nil {
			return errs.Wrap(err)
		}
		fmt.Println("For trash", forTrash)

		return nil
	}
	StoreCmd.AddCommand(cmd)
}
