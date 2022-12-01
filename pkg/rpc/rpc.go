package rpc

import (
	stbb "github.com/elek/stbb/pkg"
	"github.com/spf13/cobra"
)

var RpcCmd = &cobra.Command{
	Use: "rpc",
}

func init() {
	stbb.RootCmd.AddCommand(RpcCmd)
}
