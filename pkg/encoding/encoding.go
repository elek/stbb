package encoding

import (
	stbb "github.com/elek/stbb/pkg"
	"github.com/spf13/cobra"
)

var EncodingCmd = &cobra.Command{
	Use: "encoding",
}

func init() {
	stbb.RootCmd.AddCommand(EncodingCmd)
}
