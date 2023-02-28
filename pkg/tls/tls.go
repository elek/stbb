package tls

import (
	stbb "github.com/elek/stbb/pkg"
	"github.com/spf13/cobra"
)

var TlsCmd = &cobra.Command{
	Use: "tls",
}

func init() {
	stbb.RootCmd.AddCommand(TlsCmd)
}
