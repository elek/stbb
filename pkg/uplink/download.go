package uplink

import (
	"context"
	"github.com/elek/stbb/pkg/util"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs"
	"io"
	"os"
	"storj.io/common/rpc/rpcpool"
	"storj.io/storj/cmd/uplink/ulloc"
	"storj.io/uplink"
	"storj.io/uplink/private/transport"
	"time"
	//	"storj.io/uplink/private/testuplink"
)

func init() {
	cmd := &cobra.Command{
		Use:  "download <sj://bucket/encryptedpath> <dest>",
		Args: cobra.ExactArgs(2),
	}
	samples := cmd.Flags().IntP("samples", "n", 1, "Number of tests to be executed")
	verbose := cmd.Flags().BoolP("verbose", "v", false, "Verbose")
	pool := cmd.Flags().IntP("pool", "p", 0, "Use pool: 0 - no, 1 - common, 2 - separated pool for satellite and storagenode")
	poolSize := cmd.Flags().IntP("pool-size", "", 200, "Number of elements in the pool")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return download(args[0], args[1], *samples, *verbose, *pool, *poolSize)
	}
	UplinkCmd.AddCommand(cmd)
}

func download(from string, to string, samples int, verbose bool, pool int, poolSize int) error {
	ctx := context.Background()
	gr := os.Getenv("UPLINK_ACCESS")

	access, err := uplink.ParseAccess(gr)
	if err != nil {
		return err
	}

	p, err := ulloc.Parse(from)
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

	if pool > 0 {
		pool := rpcpool.New(rpcpool.Options{
			Name:           "uplink",
			Capacity:       poolSize,
			KeyCapacity:    5,
			IdleExpiration: 20 * time.Minute,
		})
		defer pool.Close()
		err = transport.SetConnectionPool(ctx, &cfg, pool)
		if err != nil {
			return err
		}
	}

	if pool > 1 {
		//pool := rpcpool.New(rpcpool.Options{
		//	Name:           "satellite",
		//	Capacity:       200,
		//	KeyCapacity:    10,
		//	IdleExpiration: 2 * time.Minute,
		//})
		//defer pool.Close()
		//err = transport.SetConnectionPool(ctx, &cfg, pool)
		//if err != nil {
		//	return err
		//}
	}

	_, err = util.Loop(samples, verbose, func() error {
		return downloadOne(ctx, cfg, access, bucket, key, to)
	})
	return err
}

func downloadOne(ctx context.Context, cfg uplink.Config, access *uplink.Access, bucket string, key string, to string) error {
	project, err := cfg.OpenProject(ctx, access)
	if err != nil {
		return err
	}
	defer project.Close()

	dest, err := os.Create(to)
	if err != nil {
		return err
	}
	defer dest.Close()

	source, err := project.DownloadObject(ctx, bucket, key, nil)
	if err != nil {
		return err
	}
	defer source.Close()

	_, err = io.Copy(dest, source)
	if err != nil {
		return err
	}

	return nil
}

func deleteOne(ctx context.Context, cfg uplink.Config, access *uplink.Access, bucket string, key string) error {
	project, err := cfg.OpenProject(ctx, access)
	if err != nil {
		return err
	}
	defer project.Close()

	_, err = project.DeleteObject(ctx, bucket, key)
	return err

}
