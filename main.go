package main

import (
	"context"
	"fmt"
	"github.com/alecthomas/kong"
	stbb "github.com/elek/stbb/pkg"
	"github.com/elek/stbb/pkg/downloadng"
	"github.com/elek/stbb/pkg/encoding"
	"github.com/elek/stbb/pkg/load"
	"github.com/elek/stbb/pkg/node"
	"github.com/elek/stbb/pkg/nodeid"
	"github.com/elek/stbb/pkg/piece"
	"github.com/elek/stbb/pkg/satellite"
	"github.com/elek/stbb/pkg/uplink"
	"github.com/spacemonkeygo/monkit/v3"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"storj.io/common/storj"
	jaeger "storj.io/monkit-jaeger"
	"strings"
	"sync"
	"syscall"
)

func main() {

	if os.Getenv("STBB_JAEGER") != "" {
		// agent.tracing.datasci.storj.io:5775
		serviceName := os.Getenv("STBB_SERVICE_NAME")
		if serviceName == "" {
			serviceName = "stbb"
		}
		collector, err := jaeger.NewUDPCollector(zap.L(), os.Getenv("STBB_JAEGER"), serviceName, nil, 0, 0, 0)
		if err != nil {
			panic(err)
		}
		defer func() {
			_ = collector.Close()
		}()

		defer tracked(context.Background(), collector.Run)()

		cancel := jaeger.RegisterJaeger(monkit.Default, collector, jaeger.Options{Fraction: 1})
		defer cancel()

		//var printedFirst bool
		monkit.Default.ObserveTraces(func(trace *monkit.Trace) {
			// workaround to hide the traceID of tlsopts.verifyIndentity called from a separated goroutine
			//if !printedFirst {
			fmt.Printf("trace: %x\n", trace.Id())
			//printedFirst = true
			//}
		})

	}

	if os.Getenv("STBB_PPROF") != "" {
		var output *os.File
		output, err := os.Create(os.Getenv("STBB_PPROF"))
		if err != nil {
			panic(err)
		}
		defer func() {
			output.Close()
		}()

		err = pprof.StartCPUProfile(output)
		if err != nil {
			panic(err)
		}
		defer pprof.StopCPUProfile()
	}

	if os.Getenv("STBB_MONKIT") != "" {
		filter := strings.ToLower(os.Getenv("STBB_MONKIT"))
		defer func() {
			monkit.Default.Stats(func(key monkit.SeriesKey, field string, val float64) {
				if filter == "true" || strings.Contains(strings.ToLower(key.String()), filter) {
					fmt.Println(key, field, val)
				}
			})
		}()
	}

	if os.Getenv("STBB_PPROF_ALLOCS") != "" {
		var output *os.File
		output, err := os.Create(os.Getenv("STBB_PPROF_ALLOCS"))
		if err != nil {
			panic(err)
		}
		defer func() {
			output.Close()
		}()

		defer func() {
			err = pprof.Lookup("allocs").WriteTo(output, 0)
			if err != nil {
				panic(err)
			}
		}()
	}

	usr1 := make(chan os.Signal, 1)
	defer close(usr1)
	signal.Notify(usr1, syscall.SIGUSR1)
	go func() {
		for {
			select {
			case _, ok := <-usr1:
				if !ok {
					return
				}
				fmt.Println(string(readStack()))
			}
		}
	}()

	var cli struct {
		Load       load.Load              `cmd:"" help:"Various load tests"`
		Uplink     uplink.Uplink          `cmd:"" help:"Uplink based upload/download tests"`
		Piece      piece.Piece            `cmd:""`
		Nodeid     nodeid.NodeID          `cmd:""`
		Node       node.Node              `cmd:""`
		Satellite  satellite.Satellite    `cmd:""`
		Downloadng downloadng.DownloadCmd `cmd:""`
		Encoding   encoding.Encoding      `cmd:""`
		Telemetry  stbb.TelemetryReceiver `cmd:""`
		Version    Version                `cmd:""`
	}

	ctx := kong.Parse(&cli,
		kong.TypeMapper(reflect.TypeOf(storj.NodeURL{}), kong.MapperFunc(func(ctx *kong.DecodeContext, target reflect.Value) error {
			s := ctx.Scan.Pop().Value.(string)
			url, err := storj.ParseNodeURL(s)
			if err != nil {
				return err
			}
			target.Set(reflect.ValueOf(url))
			return nil
		})),
	)

	kong.Bind(ctx)
	err := ctx.Run(ctx)
	ctx.FatalIfErrorf(err)
}

func readStack() []byte {
	buf := make([]byte, 1024)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, 2*len(buf))
	}
}

func tracked(ctx context.Context, cb func(context.Context)) (done func()) {
	ctx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		cb(ctx)
		wg.Done()
	}()

	return func() {
		cancel()
		wg.Wait()
	}
}

type Version struct {
}

func (v Version) Run() error {
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if strings.HasPrefix(s.Key, "vcs.") {
				fmt.Printf("%+v\n", s.Key+"="+s.Value)
			}
		}
		for _, m := range bi.Deps {
			if strings.Contains(m.Path, "storj.io") {
				fmt.Println(m.Path, m.Version)
			}
		}
	}
	return nil
}
