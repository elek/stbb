package rpc

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"storj.io/storj/storage"
)

func init() {
	cmd := &cobra.Command{
		Use:   "copy <from> <to>",
		Short: "Copy data from one directory to other",
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
				return copyBlob(ctx, from, to, info.BlobRef())
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

func copyBlob(ctx context.Context, from storage.Blobs, to storage.Blobs, ref storage.BlobRef) error {
	original, err := from.Open(ctx, ref)
	if err != nil {
		return err
	}
	defer original.Close()

	destination, err := to.Create(ctx, ref, -1)
	if err != nil {
		return err
	}
	_, err = io.Copy(destination, original)
	if err != nil {
		return err
	}
	return destination.Commit(ctx)

}
