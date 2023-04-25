package load

import (
	"context"
	"fmt"
	"github.com/zeebo/errs"
	"math/rand"
	"os"
	"storj.io/storj/cmd/uplink/ulloc"
	"storj.io/uplink"
	"sync"
)

type Uplink struct {
	Verbose bool   `help:"Print out more information"`
	Path    string `arg:"" name:"path" help:"path to the file to be uploaded"`
	Sample  int    `short:"n"  default:"10" help:"Number of executions ON EACH go rountie"`
	Thread  int    `short:"t"  default:"1" help:"Number of parallel Go routines"`
	Size    int    `short:"s" size:"4194304" help:"Size of the files to be uploaded"`
}

func (u *Uplink) Run() error {
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
		go func() {
			data := make([]byte, u.Size)
			_, err := rand.Read(data)
			if err != nil {
				panic(err)
			}
			for j := 0; j < u.Sample; j++ {
				if ctx.Err() != nil {
					return
				}
				keyInstance := fmt.Sprintf("%s-%d", key, 0)
				if u.Verbose {
					fmt.Println("Uploading / downloading " + keyInstance)
				}
				err := Upload(ctx, cfg, access, data, bucket, keyInstance)
				if err != nil {
					fmt.Println(err)
				}
				_, err = Download(ctx, cfg, access, bucket, keyInstance)
				if err != nil {
					fmt.Println(err)
				}
				err = Delete(ctx, cfg, access, bucket, keyInstance)
				if err != nil {
					fmt.Println(err)
				}
			}
			wg.Done()

		}()
	}
	wg.Wait()
	cancel()
	return err
}
