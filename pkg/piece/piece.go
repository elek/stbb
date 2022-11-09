package piece

import (
	stbb "github.com/elek/stbb/pkg"
	"github.com/spf13/cobra"
)

var PieceCmd = &cobra.Command{
	Use: "piece",
}

func init() {
	stbb.RootCmd.AddCommand(PieceCmd)
}
