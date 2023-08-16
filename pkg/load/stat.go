package load

import (
	"context"
	"fmt"
	"github.com/zeebo/errs"
	"os"
	"storj.io/storj/cmd/uplink/ulloc"
	"storj.io/uplink"
	"sync"
	"time"
)

type Stat struct {
	Verbose bool   `help:"Print out more information"`
	Path    string `arg:"" name:"path" help:"path to the file to be uploaded"`
	Sample  int    `short:"n"  default:"10" help:"Number of executions ON EACH go routine"`
	Thread  int    `short:"t"  default:"1" help:"Number of parallel Go routines"`
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

	wg := sync.WaitGroup{}
	wg.Add(u.Thread)
	for i := 0; i < u.Thread; i++ {
		go func(ix int) {
			for j := 0; j < u.Sample; j++ {
				if ctx.Err() != nil {
					return
				}
				u.stat(cfg, ctx, access, bucket, key)
				if j%10 == 0 {
					fmt.Println(ix, j)
				}
			}
			wg.Done()

		}(i)
	}
	wg.Wait()
	cancel()
	return nil
}

func (u *Stat) stat(cfg uplink.Config, ctx context.Context, access *uplink.Access, bucket string, key string) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	project, err := cfg.OpenProject(ctx, access)
	if err != nil {
		fmt.Println(err)
	}
	_, err = project.StatObject(ctx, bucket, key)
	if err != nil {
		fmt.Println(err)
	}
}
