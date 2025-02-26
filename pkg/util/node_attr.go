package util

import (
	"github.com/pkg/errors"
	"storj.io/storj/satellite/nodeselection"
	"strings"
)

func ParseAttributes(attr []string) (res []nodeselection.NodeAttribute, err error) {
	for _, a := range attr {
		attribute, err := nodeselection.CreateNodeAttribute(a)
		if err != nil {
			return res, errors.WithStack(err)
		}
		res = append(res, attribute)
	}
	return res, nil
}

func NodeInfo(res []nodeselection.NodeAttribute, node nodeselection.SelectedNode) string {
	var result []string
	for _, attr := range res {
		result = append(result, attr(node))
	}
	return strings.Join(result, ",")
}
