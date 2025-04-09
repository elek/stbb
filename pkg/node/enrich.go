package node

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/db"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"os"
	"storj.io/storj/satellite/nodeselection"
	"strings"
	"time"
)

type Enrich struct {
	db.WithDatabase
	File string `arg:""`
}

func (i Enrich) Run() error {
	ctx := context.Background()
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	satelliteDB, err := i.WithDatabase.GetSatelliteDB(ctx, log)

	if err != nil {
		return err
	}
	defer satelliteDB.Close()
	nodes, err := satelliteDB.OverlayCache().GetAllParticipatingNodes(ctx, 4*time.Hour, -1*time.Second)
	if err != nil {
		return errors.WithStack(err)
	}
	nodeIDs, err := os.ReadFile(i.File)
	if err != nil {
		return errors.WithStack(err)
	}

	attrs, err := util.ParseAttributes([]string{"tag:host"})
	if err != nil {
		return errors.WithStack(err)
	}

	for _, line := range strings.Split(string(nodeIDs), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		node, found := findNode(nodes, line)
		if !found {
			fmt.Println(line, "???")
		}
		fmt.Println(node.ID.String() + "," + util.NodeInfo(attrs, node))
	}
	return nil
}

func findNode(nodes []nodeselection.SelectedNode, line string) (nodeselection.SelectedNode, bool) {
	for _, node := range nodes {
		if node.ID.String() == line {
			return node, true
		}
	}
	return nodeselection.SelectedNode{}, false
}
