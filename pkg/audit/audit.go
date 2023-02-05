package audit

import (
	stbb "github.com/elek/stbb/pkg"
	"github.com/spf13/cobra"
)

var AuditCmd = &cobra.Command{
	Use: "audit",
}

func init() {
	stbb.RootCmd.AddCommand(AuditCmd)
}
