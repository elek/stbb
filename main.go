package main

import (
	"context"
	"fmt"
	stbb "github.com/elek/stbb/pkg"
	_ "github.com/elek/stbb/pkg/access"
	_ "github.com/elek/stbb/pkg/algo"
	_ "github.com/elek/stbb/pkg/audit"
	_ "github.com/elek/stbb/pkg/downloadng"
	_ "github.com/elek/stbb/pkg/encoding"
	_ "github.com/elek/stbb/pkg/load"
	_ "github.com/elek/stbb/pkg/metainfo"
	_ "github.com/elek/stbb/pkg/node"
	_ "github.com/elek/stbb/pkg/piece"
	_ "github.com/elek/stbb/pkg/rpc"
	_ "github.com/elek/stbb/pkg/satellite"
	_ "github.com/elek/stbb/pkg/store"
	_ "github.com/elek/stbb/pkg/tls"
	_ "github.com/elek/stbb/pkg/uplink"
	"github.com/spacemonkeygo/monkit/v3"
	"go.uber.org/zap"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
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
	err := stbb.RootCmd.Execute()
	if err != nil {
		log.Fatalf("%++v", err)
	}
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
