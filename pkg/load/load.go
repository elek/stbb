package uplink

import (
	stbb "github.com/elek/stbb/pkg"
	"github.com/spf13/cobra"
)

var UplinkCmd = &cobra.Command{
	Use: "load",
}

func init() {
	stbb.RootCmd.AddCommand(UplinkCmd)
}
