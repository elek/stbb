package load

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/util"
	"github.com/zeebo/errs"
	"math/rand"
	"os"
	"storj.io/storj/cmd/uplink/ulloc"
	"storj.io/uplink"
	"sync"
	"time"
)

type Uplink struct {
	Verbose        bool          `help:"Print out more information"`
	Path           string        `arg:"" name:"path" help:"path to the file to be uploaded"`
	Sample         int           `short:"n"  default:"10" help:"Number of executions ON EACH go routine"`
	Thread         int           `short:"t"  default:"1" help:"Number of parallel Go routines"`
	Size           int           `short:"s" size:"4194304" help:"Size of the files to be uploaded"`
	TTL            time.Duration `default:"0" help:"Time to live for the uploaded object"`
	EnableDownload bool          `default:"false" help:"Enable download as part of the test"`
	EnableDelete   bool          `default:"false" help:"Enable deletion as part of the test (set TTL if you don't use it'!)"`

	progress util.Progres
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

	u.progress = util.Progres{}

	bucket, key, ok := p.RemoteParts()
	if !ok {
		return errs.New("Path is not remote %s", u.Path)
	}

	cfg := uplink.Config{
		UserAgent: "stbb",
	}

	wg := &sync.WaitGroup{}
	wg.Add(u.Thread)
	for i := 0; i < u.Thread; i++ {
		go func(ix int) {
			u.Test(ix, ctx, key, cfg, access, bucket, wg)
		}(i)
	}
	wg.Wait()
	cancel()
	return err
}

func (u *Uplink) Test(ix int, ctx context.Context, key string, cfg uplink.Config, access *uplink.Access, bucket string, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()
	data := make([]byte, u.Size)
	_, err := rand.Read(data)
	if err != nil {
		panic(err)
	}

	project, err := cfg.OpenProject(ctx, access)
	if err != nil {
		panic(err)
	}
	defer project.Close()

	for j := 0; j < u.Sample; j++ {
		if ctx.Err() != nil {
			return
		}
		keyInstance := fmt.Sprintf("%s-%d-%d", key, ix, j)
		if u.Verbose {
			fmt.Println("Uploading / downloading " + keyInstance)
		}
		err := Upload(ctx, project, data, bucket, keyInstance, u.TTL)
		if err != nil {
			fmt.Println(err)
		}
		if u.EnableDownload {
			_, err = Download(ctx, project, bucket, keyInstance)
			if err != nil {
				fmt.Println(err)
			}
		}
		if u.EnableDelete {
			err = Delete(ctx, project, bucket, keyInstance)
			if err != nil {
				fmt.Println(err)
			}
		}
		u.progress.Increment()
	}

}
