package main

import (
	"fmt"
	stbb "github.com/elek/stbb/pkg"
	_ "github.com/elek/stbb/pkg/algo"
	_ "github.com/elek/stbb/pkg/downloadng"
	_ "github.com/elek/stbb/pkg/encoding"
	_ "github.com/elek/stbb/pkg/node"
	_ "github.com/elek/stbb/pkg/piece"
	_ "github.com/elek/stbb/pkg/rpc"
	_ "github.com/elek/stbb/pkg/satellite"
	_ "github.com/elek/stbb/pkg/store"
	_ "github.com/elek/stbb/pkg/tls"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
)

func main() {

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
