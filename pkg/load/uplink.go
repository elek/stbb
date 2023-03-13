package uplink

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs"
	"math/rand"
	"os"
	"storj.io/storj/cmd/uplink/ulloc"
	"storj.io/uplink"
	"sync"
	"time"
)

func init() {
	cmd := &cobra.Command{
		Use:  "uplink <sj://bucket/encryptedpath>",
		Args: cobra.ExactArgs(1),
	}
	verbose := cmd.Flags().BoolP("verbose", "v", false, "Verbose")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		rand.Seed(time.Now().Unix())
		return uplinkLoad(args[0], *verbose)
	}
	UplinkCmd.AddCommand(cmd)
}

type UplinkLoad struct {
	Verbose bool
}

func uplinkLoad(path string, verbose bool) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gr := os.Getenv("UPLINK_ACCESS")

	access, err := uplink.ParseAccess(gr)
	if err != nil {
		return err
	}

	p, err := ulloc.Parse(path)
	if err != nil {
		return err
	}

	bucket, key, ok := p.RemoteParts()
	if !ok {
		return errs.New("Path is not remote %s", path)
	}

	cfg := uplink.Config{
		UserAgent: "stbb",
	}

	wg := sync.WaitGroup{}
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			data := make([]byte, 4*1024*1024)
			_, err := rand.Read(data)
			if err != nil {
				panic(err)
			}
			for {
				if ctx.Err() != nil {
					return
				}
				keyInstance := fmt.Sprintf("%s-%d", key, rand.Int63())
				if verbose {
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

				wg.Done()
			}

		}()
	}
	wg.Wait()
	cancel()
	return err
}
