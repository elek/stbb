package rpc

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:   "namespaces <dir>",
		Short: "List namespaces from a storage dir",
		Args:  cobra.ExactArgs(1),
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		blobs, err := createBlobs(args[0])
		if err != nil {
			return err
		}
		ids, err := blobs.ListNamespaces(ctx)
		if err != nil {
			return err
		}
		for _, id := range ids {
			fmt.Println(hex.EncodeToString(id))
		}
		return nil
	}
	StoreCmd.AddCommand(cmd)
}
