package main

import (
	stbb "github.com/elek/stbb/pkg"
	_ "github.com/elek/stbb/pkg/piece"
	_ "github.com/elek/stbb/pkg/rpc"
	_ "github.com/elek/stbb/pkg/tls"
	"log"
)

func main() {
	err := stbb.RootCmd.Execute()
	if err != nil {
		log.Fatalf("%++v", err)
	}
}
