package uplink

import (
	"context"
	"github.com/elek/stbb/pkg/util"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs"
	"io"
	"os"
	"storj.io/storj/cmd/uplink/ulloc"
	"storj.io/uplink"
	"storj.io/uplink/private/testuplink"
)

func init() {
	cmd := &cobra.Command{
		Use:  "upload <source> <sj://bucket/encryptedpath>",
		Args: cobra.ExactArgs(2),
	}
	samples := cmd.Flags().IntP("samples", "n", 1, "Number of tests to be executed")
	verbose := cmd.Flags().BoolP("verbose", "v", false, "Verbose")
	refactored := cmd.Flags().BoolP("refactored", "r", false, "User refactored code path")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		_, err := util.Loop(*samples, *verbose, func() error {
			return upload(args[0], args[1], *refactored)
		})
		return err
	}
	UplinkCmd.AddCommand(cmd)
}

func upload(from string, to string, refactored bool) error {
	ctx := context.Background()

	if refactored {
		ctx = testuplink.WithConcurrentSegmentUploadsDefaultConfig(ctx)
	}
	gr := os.Getenv("UPLINK_ACCESS")

	access, err := uplink.ParseAccess(gr)
	if err != nil {
		return err
	}

	p, err := ulloc.Parse(to)
	if err != nil {
		return err
	}

	bucket, key, ok := p.RemoteParts()
	if !ok {
		return errs.New("Path is not remote %s", to)
	}

	cfg := uplink.Config{
		UserAgent: "stbb",
	}
	project, err := cfg.OpenProject(ctx, access)
	if err != nil {
		return err
	}

	source, err := os.Open(from)
	if err != nil {
		return err
	}

	dest, err := project.UploadObject(ctx, bucket, key, &uplink.UploadOptions{})
	if err != nil {
		return err
	}

	_, err = io.Copy(dest, source)
	if err != nil {
		return err
	}

	err = dest.Commit()
	if err != nil {
		return err
	}

	return nil
}
