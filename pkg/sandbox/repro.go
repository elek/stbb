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

type Sandbox struct {
	Repro Repro `cmd:""`
	Stat  Stat  `cmd:""`
}
type Repro struct {
	Path string `arg:"" name:"path" help:"path to the file to be downloaded"`
}

func (u *Repro) Run() error {
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
		Capacity:       10,
		KeyCapacity:    10,
		IdleExpiration: 30 * time.Second,
		Name:           "satellite",
	}))
	if err != nil {
		return err
	}

	err = transport.SetConnectionPool(context.TODO(), &cfg, rpcpool.New(rpcpool.Options{
		Capacity:       10,
		KeyCapacity:    10,
		IdleExpiration: 30 * time.Second,
		Name:           "pool",
	}))
	if err != nil {
		return err
	}

	for i := 0; i < 10; i++ {
		project, err := cfg.OpenProject(ctx, access)
		if err != nil {
			return errs.Wrap(err)
		}
		defer project.Close()

		cctx, cancel := context.WithCancel(ctx)
		object, err := project.DownloadObject(cctx, bucket, key, nil)
		if err != nil {
			return errs.Wrap(err)
		}

		all, err := object.Read(make([]byte, 800))
		if err != nil {
			return errs.Wrap(err)
		}
		cancel()
		fmt.Println(all)
		object.Close()
	}
	time.Sleep(1 * time.Hour)

	return nil
}
