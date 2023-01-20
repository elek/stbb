package rpc

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"storj.io/storj/storage/filestore"
)

func init() {
	cmd := &cobra.Command{
		Use:   "namespaces <dir>",
		Short: "List namespaces from a storage dir",
		Args:  cobra.ExactArgs(1),
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		store, err := filestore.NewDir(zap.NewNop(), args[0])
		if err != nil {
			return err
		}
		ids, err := store.ListNamespaces(ctx)
		if err != nil {
			return err
		}
		for _, id := range ids {
			fmt.Println(id)
		}
		return nil
	}
	StoreCmd.AddCommand(cmd)
}
