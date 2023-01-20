package rpc

import (
	stbb "github.com/elek/stbb/pkg"
	"github.com/spf13/cobra"
)

var StoreCmd = &cobra.Command{
	Use: "store",
}

func init() {
	stbb.RootCmd.AddCommand(StoreCmd)
}
