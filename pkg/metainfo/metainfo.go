package piece

import (
	stbb "github.com/elek/stbb/pkg"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/spf13/cobra"
)

var mon = monkit.Package()
var MetainfoCmd = &cobra.Command{
	Use:   "metainfo",
	Short: "commands related to the satellite's meatinfo API",
}

func init() {
	stbb.RootCmd.AddCommand(MetainfoCmd)
}
