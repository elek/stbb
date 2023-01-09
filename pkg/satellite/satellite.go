package satellite

import (
	stbb "github.com/elek/stbb/pkg"
	"github.com/spf13/cobra"
)

var SatelliteCmd = &cobra.Command{
	Use:   "satellite",
	Short: "Mock satellite for testing real storagenode with satellite",
}

func init() {
	stbb.RootCmd.AddCommand(SatelliteCmd)
}
