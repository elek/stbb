package main

import (
	"context"
	"fmt"
	"github.com/alecthomas/kong"
	stbb "github.com/elek/stbb/pkg"
	"github.com/elek/stbb/pkg/access"
	"github.com/elek/stbb/pkg/admin"
	"github.com/elek/stbb/pkg/audit"
	"github.com/elek/stbb/pkg/authservice"
	"github.com/elek/stbb/pkg/bloom"
	"github.com/elek/stbb/pkg/config"
	"github.com/elek/stbb/pkg/crypto"
	"github.com/elek/stbb/pkg/downloadng"
	"github.com/elek/stbb/pkg/hashstore"
	"github.com/elek/stbb/pkg/jobq"
	"github.com/elek/stbb/pkg/load"
	"github.com/elek/stbb/pkg/metabase"
	"github.com/elek/stbb/pkg/metainfo"
	"github.com/elek/stbb/pkg/node"
	"github.com/elek/stbb/pkg/nodeid"
	"github.com/elek/stbb/pkg/piece"
	"github.com/elek/stbb/pkg/placement"
	"github.com/elek/stbb/pkg/rangedloop"
	"github.com/elek/stbb/pkg/rpc"
	"github.com/elek/stbb/pkg/sandbox"
	"github.com/elek/stbb/pkg/satellite"
	"github.com/elek/stbb/pkg/satellitedb"
	"github.com/elek/stbb/pkg/segment"
	"github.com/elek/stbb/pkg/store"
	"github.com/elek/stbb/pkg/uplink"
	"github.com/spacemonkeygo/monkit/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"net"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	dbg "storj.io/common/debug"
	"storj.io/common/storj"
	jaeger "storj.io/monkit-jaeger"
	"strings"
	"sync"
)

func main() {
	logCfg := zap.NewDevelopmentConfig()
	logCfg.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	zapLog, err := logCfg.Build()
	if err != nil {
		panic(err)
	}
	defer zapLog.Sync()

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

	debugListAddress := os.Getenv("STBB_DEBUG")
	if debugListAddress != "" {
		fmt.Println("starting debug server ", debugListAddress)
		listener, err := net.Listen("tcp", debugListAddress)
		if err != nil {
			panic(err)
		}
		dbgServer := dbg.NewServer(zapLog, listener, monkit.Default, dbg.Config{})
		go func() {
			err := dbgServer.Run(context.Background())
			if err != nil {
				fmt.Println(err)
			}
		}()
		defer dbgServer.Close()
	}

	defer initSignal()()

	var cli struct {
		Load        load.Load               `cmd:"" help:"Various load tests"`
		Uplink      uplink.Uplink           `cmd:"" help:"Uplink based upload/download tests"`
		Piece       piece.Piece             `cmd:""`
		Nodeid      nodeid.NodeID           `cmd:""`
		Node        node.Node               `cmd:""`
		Satellite   satellite.Satellite     `cmd:""`
		Downloadng  downloadng.DownloadCmd  `cmd:""`
		Telemetry   stbb.TelemetryReceiver  `cmd:""`
		Version     Version                 `cmd:""`
		Access      access.AccessCmd        `cmd:""`
		RPC         rpc.RPC                 `cmd:""`
		Crypto      crypto.Crypto           `cmd:""`
		GeoIP       GeoIP                   `cmd:""`
		RangedLoop  rangedloop.RangedLoop   `cmd:""`
		Sandbox     sandbox.Sandbox         `cmd:""`
		Segment     segment.Segment         `cmd:""`
		Metainfo    metainfo.Metainfo       `cmd:""`
		Bloom       bloom.Bloom             `cmd:"helpers to process bloom filters"`
		Store       store.Store             `cmd:""`
		IOTest      IOTest                  `cmd:""`
		Placement   placement.Placement     `cmd:"placement (and node selection) based helpers"`
		BadgerGet   authservice.ReadAuth    `cmd:"" help:"read grant from Badger based authservice database"`
		Metabase    metabase.Metabase       `cmd:"" help:"Raw metabase db related helpers"`
		Admin       admin.Admin             `cmd:"" help:"helper commands, similar to the admin interface"`
		Hashstore   hashstore.Hashstore     `cmd:"" help:"commands related to the new hashtable based store"`
		Audit       audit.Audit             `cmd:"" help:"commands related to the audit subsystem"`
		Jobq        jobq.Jobq               `cmd:"" help:"jobq related helper commands"`
		SatelliteDB satellitedb.SatelliteDB `cmd:"" help:"queries and updates related to the satellite database"`
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
		kong.Configuration(config.Loader, "~/.config/stbb/config.yaml"),
	)

	kong.Bind(ctx)
	err = ctx.Run(ctx)
	if err != nil {
		log.Fatalf("%+v", err)
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
