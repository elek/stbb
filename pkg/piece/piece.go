package piece

import (
	stbb "github.com/elek/stbb/pkg"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/spf13/cobra"
)

var mon = monkit.Package()

var PieceCmd = &cobra.Command{
	Use: "piece",
}

func init() {
	stbb.RootCmd.AddCommand(PieceCmd)
}
