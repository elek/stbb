package uplink

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs"
	"golang.org/x/exp/rand"
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
		Use:  "updown <file> <sj://bucket/encryptedpath> ",
		Args: cobra.ExactArgs(2),
	}
	samples := cmd.Flags().IntP("samples", "n", 1, "Number of tests to be executed")
	verbose := cmd.Flags().BoolP("verbose", "v", false, "Verbose")
	pool := cmd.Flags().IntP("pool", "p", 0, "Use pool: 0 - no, 1 - common, 2 - separated pool for satellite and storagenode")
	poolSize := cmd.Flags().IntP("pool-size", "", 200, "Number of elements in the pool")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return updown(args[0], args[1], *samples, *verbose, *pool, *poolSize)
	}
	UplinkCmd.AddCommand(cmd)
}

func updown(fileName string, keyBucket string, samples int, verbose bool, pool int, poolSize int) error {
	ctx := context.Background()
	gr := os.Getenv("UPLINK_ACCESS")

	access, err := uplink.ParseAccess(gr)
	if err != nil {
		return err
	}

	p, err := ulloc.Parse(keyBucket)
	if err != nil {
		return err
	}

	bucket, key, ok := p.RemoteParts()
	if !ok {
		return errs.New("Path is not remote %s", keyBucket)
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
		currentKey := fmt.Sprintf("%s-%d", key, rand.Int63())
		if verbose {
			fmt.Println("Key name:", currentKey)
		}
		err := uploadOne(ctx, cfg, access, fileName, bucket, currentKey)
		if err != nil {
			return err
		}

		err = downloadOne(ctx, cfg, access, bucket, currentKey, fileName+".down")
		if err != nil {
			return err
		}

		err = deleteOne(ctx, cfg, access, bucket, currentKey)
		if err != nil {
			return err
		}
		return nil
	})
	return err

}
