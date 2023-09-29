package sandbox

import (
	"context"
	"fmt"
	"github.com/zeebo/errs"
	"math/rand"
	"os"
	"storj.io/common/rpc/rpcpool"
	"storj.io/storj/cmd/uplink/ulloc"
	"storj.io/uplink"
	"storj.io/uplink/private/transport"
	"time"
)

type Sandbox struct {
	Repro Repro `cmd:""`
}
type Repro struct {
	Path string `arg:"" name:"path" help:"path to the file to be uploaded"`
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
		Capacity:       2,
		KeyCapacity:    2,
		IdleExpiration: 0,
		Name:           "pool",
	}))
	if err != nil {
		return err
	}

	for i := 0; i < 100; i++ {
		u.stat(cfg, ctx, access, bucket, key)
	}

	return nil
}

func (u *Repro) stat(cfg uplink.Config, ctx context.Context, access *uplink.Access, bucket string, key string) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if rand.Intn(2) > 0 {
		go func() {
			time.Sleep(120 * time.Millisecond)
			cancel()
		}()
	}

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
