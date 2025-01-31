package util

import (
	"fmt"
	"storj.io/storj/satellite/nodeselection"
	"strings"
)

func PrintHistogram(nodes []*nodeselection.SelectedNode, selector ...nodeselection.NodeAttribute) {
	key := func(node *nodeselection.SelectedNode) string {
		var k []string
		for _, attr := range selector {
			k = append(k, attr(*node))
		}
		return strings.Join(k, ",")
	}
	histogram := map[string]int{}
	for _, n := range nodes {
		c := key(n)
		histogram[c] = histogram[c] + 1
	}
	fmt.Print("Selected nodes: \n")
	for k, v := range histogram {
		fmt.Printf("   %s:%d\n", k, v)
	}
	fmt.Println()
}
