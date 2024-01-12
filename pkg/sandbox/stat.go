package sandbox

import (
	"context"
	"fmt"
	"github.com/zeebo/errs"
	"os"
	"storj.io/common/rpc/rpcpool"
	"storj.io/storj/cmd/uplink/ulloc"
	"storj.io/uplink"
	"storj.io/uplink/private/transport"
	"time"
)

type Stat struct {
	Path      string `arg:"" name:"path" help:"path to the file to be uploaded"`
	MaxPeriod int    `arg:"max time between two request in minutes"`
}

func (u *Stat) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gr := os.Getenv("UPLINK_ACCESS")
	access, err := uplink.ParseAccess(gr)
	if err != nil {
		return err
	}

	p, err := ulloc.Parse(u.Path)
	if err != nil {
		return err
	}

	bucket, key, ok := p.RemoteParts()
	if !ok {
		return errs.New("Path is not remote %s", u.Path)
	}

	cfg := uplink.Config{
		UserAgent: "stbb",
	}

	err = transport.SetSatelliteConnectionPool(context.TODO(), &cfg, rpcpool.New(rpcpool.Options{
		Capacity:       200,
		KeyCapacity:    0,
		IdleExpiration: 0,
		Name:           "satellite",
	}))
	if err != nil {
		return err
	}

	err = transport.SetConnectionPool(context.TODO(), &cfg, rpcpool.New(rpcpool.Options{
		Capacity:       10,
		KeyCapacity:    10,
		IdleExpiration: 30,
		Name:           "pool",
	}))
	if err != nil {
		return err
	}

	period := 5 * time.Second
	for i := 0; i < 100; i++ {
		u.stat(cfg, ctx, access, bucket, key)
		time.Sleep(period)
		period = period * 3 / 2
		if period > time.Duration(u.MaxPeriod)*time.Minute {
			period = time.Duration(u.MaxPeriod) * time.Minute
		}
	}

	return nil
}

func (u *Stat) stat(cfg uplink.Config, ctx context.Context, access *uplink.Access, bucket string, key string) {
	project, err := cfg.OpenProject(ctx, access)
	if err != nil {
		fmt.Println(err)
	}
	defer project.Close()
	fmt.Println(time.Now(), "executing stat")
	_, err = project.StatObject(ctx, bucket, key)
	fmt.Println(time.Now(), "stat done")
	if err != nil {
		fmt.Println(err)
	}
}
