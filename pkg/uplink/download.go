package uplink

import (
	"context"
	"github.com/elek/stbb/pkg/util"
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

type Download struct {
	util.Loop
	Source   string `arg:"" name:"source"`
	Target   string `arg:"" name:"target"`
	Pool     int    `short:"p" help:"Use pool: 0 - no, 1 - common, 2 - separated pool for satellite and storagenode"`
	PoolSize int    `default:"200" help:"size of the connection pool"`
}

func (d *Download) Run() error {

	ctx := context.Background()
	gr := os.Getenv("UPLINK_ACCESS")

	access, err := uplink.ParseAccess(gr)
	if err != nil {
		return err
	}

	p, err := ulloc.Parse(d.Source)
	if err != nil {
		return err
	}

	bucket, key, ok := p.RemoteParts()
	if !ok {
		return errs.New("Path is not remote %s", d.Source)
	}

	cfg := uplink.Config{
		UserAgent: "stbb",
	}

	if d.Pool > 0 {
		pool := rpcpool.New(rpcpool.Options{
			Name:           "uplink",
			Capacity:       d.PoolSize,
			KeyCapacity:    5,
			IdleExpiration: 20 * time.Minute,
		})
		defer pool.Close()
		err = transport.SetConnectionPool(ctx, &cfg, pool)
		if err != nil {
			return err
		}
	}

	if d.Pool > 1 {
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

	_, err = d.Loop.Run(func() error {
		return downloadOne(ctx, cfg, access, bucket, key, d.Target)
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
