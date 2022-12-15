package node

import (
	stbb "github.com/elek/stbb/pkg"
	"github.com/spf13/cobra"
)

var NodeCmd = &cobra.Command{
	Use: "node",
}

func init() {
	stbb.RootCmd.AddCommand(NodeCmd)
}
