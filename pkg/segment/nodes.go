package segment

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elek/stbb/pkg/db"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"storj.io/common/storj"
	nodeselection2 "storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/overlay"
	"strings"
	"time"
)

type Nodes struct {
	db.WithDatabase
	NodeIDs string `arg:""`
	Verbose bool   `default:"true"`
}

func (h Nodes) Run() error {
	ctx := context.Background()
	log, err := zap.NewDevelopment()

	satelliteDB, err := h.GetSatelliteDB(ctx, log)
	if err != nil {
		return err
	}
	defer satelliteDB.Close()

	n1, n2, err := satelliteDB.OverlayCache().SelectAllStorageNodesUpload(ctx, overlay.NodeSelectionConfig{
		OnlineWindow: 100 * 24 * time.Hour,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	nodes := append(n1, n2...)

	fmt.Println("node cache is loaded", len(nodes))
	for _, node := range strings.Split(h.NodeIDs, ",") {
		id, err := storj.NodeIDFromString(node)
		if err != nil {
			return errors.WithStack(err)
		}
		selectedNode := findNode(nodes, id)
		if selectedNode == nil {
			fmt.Println("missing", id)
			continue
		}
		if h.Verbose {
			jr, err := json.MarshalIndent(selectedNode, "", " ")
			if err != nil {
				return errors.WithStack(err)
			}
			fmt.Println(string(jr))
		} else {
			fmt.Println(selectedNode.ID.String(), selectedNode.Address.Address, selectedNode.LastIPPort, printTags(selectedNode.Tags))
		}

	}

	return nil
}

func printTags(tags nodeselection2.NodeTags) string {
	if tags == nil {
		return ""
	}
	var res []string
	for _, tag := range tags {
		res = append(res, tag.Name+"="+string(tag.Value))
	}
	return strings.Join(res, ",")
}
