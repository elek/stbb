package rpc

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"storj.io/storj/storage"
)

func init() {
	cmd := &cobra.Command{
		Use:   "move <from> <to>",
		Short: "Move data from one directory to other",
		Args:  cobra.ExactArgs(2),
	}
	satellite := cmd.Flags().String("satellite", "", "Satellite selector")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		filter := NewSatelliteFilter(*satellite)
		from, err := createBlobs(args[0])
		if err != nil {
			return err
		}

		to, err := createBlobs(args[1])
		if err != nil {
			return err
		}

		ids, err := from.ListNamespaces(ctx)
		if err != nil {
			return err
		}

		for _, id := range ids {
			if !filter.Match(id) {
				continue
			}

			var infos []storage.BlobInfo
			size := int64(0)
			err = from.WalkNamespace(ctx, id, func(info storage.BlobInfo) error {
				err := copyBlob(ctx, from, to, info.BlobRef())
				if err != nil {
					return err
				}
				err = from.Delete(ctx, info.BlobRef())
				if err != nil {
					return err
				}
				return nil
			})
			fmt.Println(len(infos))
			fmt.Println(size)
			if err != nil {
				return err
			}
		}
		return nil
	}
	StoreCmd.AddCommand(cmd)
}
