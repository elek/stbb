package uplink

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
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

type UpDown struct {
	util.Loop
	Source   string `arg:"" name:"source"`
	Target   string `arg:"" name:"target"`
	Pool     int    `short:"p" help:"Use pool: 0 - no, 1 - common, 2 - separated pool for satellite and storagenode"`
	PoolSize int    `default:"200" help:"size of the connection pool"`
}

func (u *UpDown) Run() error {
	ctx := context.Background()
	gr := os.Getenv("UPLINK_ACCESS")

	access, err := uplink.ParseAccess(gr)
	if err != nil {
		return err
	}

	p, err := ulloc.Parse(u.Target)
	if err != nil {
		return err
	}

	bucket, key, ok := p.RemoteParts()
	if !ok {
		return errs.New("Path is not remote %s", u.Target)
	}

	cfg := uplink.Config{
		UserAgent: "stbb",
	}

	if u.Pool > 0 {
		pool := rpcpool.New(rpcpool.Options{
			Name:           "uplink",
			Capacity:       u.PoolSize,
			KeyCapacity:    5,
			IdleExpiration: 20 * time.Minute,
		})
		defer pool.Close()
		err = transport.SetConnectionPool(ctx, &cfg, pool)
		if err != nil {
			return err
		}
	}

	if u.PoolSize > 1 {
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

	_, err = u.Loop.Run(func() error {
		currentKey := fmt.Sprintf("%s-%d", key, rand.Int63())
		if u.Verbose {
			fmt.Println("key name:", currentKey)
		}
		err := uploadOne(ctx, cfg, access, u.Source, bucket, currentKey)
		if err != nil {
			return err
		}

		err = downloadOne(ctx, cfg, access, bucket, currentKey, u.Source+".down")
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
