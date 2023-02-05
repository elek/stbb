package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"storj.io/common/storj"
	"storj.io/storj/storage"
)

func init() {
	cmd := &cobra.Command{
		Use:   "stat <dir>",
		Short: "Statistics per namespaces",
		Args:  cobra.ExactArgs(1),
	}
	satellite := cmd.Flags().String("satellite", "", "Satellite selector")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		blobs, err := createBlobs(args[0])
		if err != nil {
			return err
		}
		filter := NewSatelliteFilter(*satellite)
		ids, err := blobs.ListNamespaces(ctx)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if !filter.Match(id) {
				continue
			}

			satelliteID, err := storj.NodeIDFromBytes(id)
			if err != nil {
				return err
			}

			var infos []storage.BlobInfo
			size := int64(0)
			err = blobs.WalkNamespace(ctx, id, func(info storage.BlobInfo) error {
				infos = append(infos, info)
				stat, err := info.Stat(ctx)
				if err != nil {
					return err
				}
				size += stat.Size()
				return nil
			})
			if err != nil {
				return err
			}
			stat := Stat{
				ID:    satelliteID.String(),
				Blobs: len(infos),
				Size:  size,
			}
			raw, err := json.Marshal(stat)
			if err != nil {
				return err
			}
			fmt.Println(string(raw))

		}
		return nil
	}
	StoreCmd.AddCommand(cmd)
}

type Stat struct {
	ID    string
	Blobs int
	Size  int64
}
