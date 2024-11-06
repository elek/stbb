package util

import (
	"fmt"
	"storj.io/storj/satellite/nodeselection"
)

func PrintHistogram(nodes []*nodeselection.SelectedNode, selector nodeselection.NodeAttribute) {
	histogram := map[string]int{}
	for _, n := range nodes {
		c := selector(*n)
		histogram[c] = histogram[c] + 1
	}
	fmt.Print("Selected nodes: \n")
	for k, v := range histogram {
		fmt.Printf("   %s:%d\n", k, v)
	}
	fmt.Println()
}
