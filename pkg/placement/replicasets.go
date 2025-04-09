package placement

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/db"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"sort"
	"storj.io/common/memory"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/overlay"
	"time"
)

func SelectWithReplicaset() {

}

type Replicasets struct {
	db.WithDatabase
	Attribute string `default:"tag:host"`
	Filter    string
}

func (r Replicasets) Run() error {
	ctx := context.Background()

	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	satelliteDB, err := r.WithDatabase.GetSatelliteDB(ctx, log)
	if err != nil {
		return err
	}
	defer satelliteDB.Close()

	oldNodes, newNodes, err := satelliteDB.OverlayCache().SelectAllStorageNodesUpload(ctx, overlay.NodeSelectionConfig{
		OnlineWindow:     4 * time.Hour,
		MinimumDiskSpace: 500 * memory.GB,
	})

	if err != nil {
		return errors.WithStack(err)
	}
	nodes := append(oldNodes, newNodes...)

	nf, err := nodeselection.FilterFromString(r.Filter, nodeselection.NewPlacementConfigEnvironment(nil, nil))
	if err != nil {
		return errors.WithStack(err)
	}
	var filtered []*nodeselection.SelectedNode
	for _, node := range nodes {
		if nf.Match(node) {
			filtered = append(filtered, node)
		}
	}

	value, err := nodeselection.CreateNodeValue("free_disk")
	if err != nil {
		return errors.WithStack(err)
	}

	sort.Slice(filtered, func(i, j int) bool {
		return value(*filtered[i]) > value(*filtered[j])
	})

	attr, err := nodeselection.CreateNodeAttribute(r.Attribute)
	if err != nil {
		return errors.WithStack(err)
	}

	rs := InitReplicasets(filtered, 17, Unique(attr))

	closedNo := 0
	closedNodes := 0
	for _, r := range rs {
		if r.IsFull() {
			closedNo++
			closedNodes += r.Len()
		}
	}

	fmt.Println("Nodes", len(filtered))
	fmt.Println("Replicasets", len(rs))
	fmt.Println("Closed sets", closedNo)
	fmt.Println("Unused nodes", len(filtered)-closedNodes)

	serverName, _ := nodeselection.CreateNodeAttribute("tag:host")
	instance, _ := nodeselection.CreateNodeAttribute("tag:service")
	serverGroup, _ := nodeselection.CreateNodeAttribute("tag:server_group")
	for _, set := range rs {

		score := 0
		for _, node := range set.Nodes {
			fmt.Println(node.ID, serverGroup(*node), serverName(*node), instance(*node))
			if int(value(*node)) > score {
				score = int(value(*node))
			}
			//score += int(value(*node))
		}
		//fmt.Println("score", float64(score/int(memory.GB))/float64(len(set.Nodes)))
		fmt.Println("score", score/int(memory.GB))
		fmt.Println()
	}
	return nil
}
