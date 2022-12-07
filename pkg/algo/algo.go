package piece

import (
	stbb "github.com/elek/stbb/pkg"
	"github.com/spf13/cobra"
)

var AlgoCmd = &cobra.Command{
	Use:   "algo",
	Short: "Test performance of different algorithms what we use (Reed-Solomon / AES-GCM / ...)",
}

func init() {
	stbb.RootCmd.AddCommand(AlgoCmd)
}
