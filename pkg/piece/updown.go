package piece

import (
	"context"
	"github.com/spf13/cobra"
	"os"
	"storj.io/common/pb"
)

func init() {
	cmd := &cobra.Command{
		Use:  "updown <storagenode-id> <file>",
		Args: cobra.ExactArgs(2),
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		u, err := NewDRPCUploader(ctx, args[0], false, pb.PieceHashAlgorithm_SHA256, false)
		if err != nil {
			return err
		}

		_, pieceID, err := u.Upload(ctx, args[1])
		if err != nil {
			return err
		}

		stat, err := os.Stat(args[1])
		if err != nil {
			return err
		}

		u.Close()

		d, err := NewDRPCDownloader(ctx, args[0], false)
		if err != nil {
			return err
		}

		dest, err := os.Create(args[1] + ".dest")
		if err != nil {
			return err
		}
		defer dest.Close()

		_, _, err = d.Download(ctx, pieceID.String(), stat.Size(), func(bytes []byte) {
			_, err = dest.Write(bytes)
			if err != nil {
				panic(err)
			}
		})
		if err != nil {
			return err
		}
		d.Close()

		return nil
	}
	PieceCmd.AddCommand(cmd)
}
