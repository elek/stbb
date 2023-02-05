package rpc

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"storj.io/common/storj"
	"storj.io/storj/storage"
)

func init() {
	cmd := &cobra.Command{
		Use:   "cat <from> <satellite> <key> ",
		Short: "Read data from one key",
		Args:  cobra.ExactArgs(3),
	}
	verbose := cmd.Flags().Bool("verbose", true, "Print out sizes and checksum")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		from, err := createBlobs(args[0])
		if err != nil {
			return err
		}

		pieceID, err := hex.DecodeString(args[2])
		if err != nil {
			return err
		}

		satelliteID, err := storj.NodeIDFromString(args[1])
		if err != nil {
			return err
		}

		f, err := from.Open(ctx, storage.BlobRef{
			Namespace: satelliteID.Bytes(),
			Key:       pieceID,
		})
		defer f.Close()
		raw, err := io.ReadAll(f)
		if err != nil {
			return err
		}
		if !*verbose {
			fmt.Println(hex.EncodeToString(raw))
		} else {
			for i, b := range raw {
				fmt.Printf("%04d %x\n", i, b)
			}
		}

		return nil
	}
	StoreCmd.AddCommand(cmd)
}
