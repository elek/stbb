package node

import (
	"encoding/csv"
	"github.com/spf13/cobra"
	"os"
)

func init() {
	cmd := &cobra.Command{
		Use:   "transform <nodes.csv>",
		Short: "Test performance of each nodes, one bye one",
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		c := csv.NewWriter(os.Stdout)
		defer c.Flush()
		err := c.Write([]string{
			"id",
			"last_net",
		})
		if err != nil {
			return err
		}
		return forEachNode(args[0], func(node NodeInfo) error {
			return c.Write([]string{
				node.NodeID.String(),
				node.LastNet,
			})
		})
	}
	NodeCmd.AddCommand(cmd)
}
