package main

import (
	stbb "github.com/elek/stbb/pkg"
	_ "github.com/elek/stbb/pkg/algo"
	_ "github.com/elek/stbb/pkg/downloadng"
	_ "github.com/elek/stbb/pkg/piece"
	_ "github.com/elek/stbb/pkg/rpc"
	_ "github.com/elek/stbb/pkg/tls"
	"log"
	"os"
	"runtime/pprof"
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

	err := stbb.RootCmd.Execute()
	if err != nil {
		log.Fatalf("%++v", err)
	}
}
