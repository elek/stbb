package rpc

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"storj.io/storj/storage"
)

func init() {
	cmd := &cobra.Command{
		Use:   "read-all <from>",
		Short: "Read all data",
		Args:  cobra.ExactArgs(1),
	}
	satellite := cmd.Flags().String("satellite", "", "Satellite selector")
	verbose := cmd.Flags().Bool("verbose", true, "Print out sizes and checksum")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		filter := NewSatelliteFilter(*satellite)
		from, err := createBlobs(args[0])
		if err != nil {
			return err
		}

		ids, err := from.ListNamespaces(ctx)
		if err != nil {
			return err
		}

		size := 0
		for _, id := range ids {
			if !filter.Match(id) {
				continue
			}
			err = from.WalkNamespace(ctx, id, func(info storage.BlobInfo) error {
				data, err := read(ctx, from, info.BlobRef())
				if err != nil {
					return err
				}
				size += len(data)
				if *verbose {
					fmt.Printf("%s %d %x\n", hex.EncodeToString(info.BlobRef().Key), len(data), md5.Sum(data))
				}
				return nil
			})
			if err != nil {
				return err
			}

		}
		if !*verbose {
			fmt.Println(size)
		}
		return nil
	}
	StoreCmd.AddCommand(cmd)
}

func read(ctx context.Context, from storage.Blobs, ref storage.BlobRef) ([]byte, error) {
	reader, err := from.Open(ctx, ref)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return readALl(reader)
}

func readALl(r io.Reader) ([]byte, error) {
	b := make([]byte, 0, 2500000)
	for {
		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return b, err
		}
	}
}
