package placement

import (
	"context"
	"fmt"
	"github.com/pkg/errors"

	"go.uber.org/zap"
	"math/rand"
	"os"
	"sort"
	"storj.io/common/memory"
	"storj.io/common/storj"

	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/overlay"
	"storj.io/storj/satellite/satellitedb"
	"strings"
	"time"
)

type SelectPool struct {
	PlacementConfig string
	Placement       storj.PlacementConstraint
	Selector        string
}

func (n *SelectPool) Run() (err error) {
	ctx := context.Background()
	selectorDef := n.Selector
	if selectorDef == "" {
		selectorDef = `tag:provider`
	}

	var attributes []nodeselection.NodeAttribute
	for _, attr := range strings.Split(selectorDef, ",") {
		attr, err := nodeselection.CreateNodeAttribute(attr)
		if err != nil {
			return errors.WithStack(err)
		}
		attributes = append(attributes, attr)
	}
	nodeAttribute := func(n nodeselection.SelectedNode) string {
		var result []string
		for _, attr := range attributes {
			result = append(result, attr(n))
		}
		return strings.Join(result, ",")
	}

	//tracker, b := metainfo.GetNewSuccessTracker("bitshift")
	//if !b {
	//	panic("unknown tracker")
	//}
	//successTrackers := metainfo.NewSuccessTrackers([]storj.nodeID{}, tracker)
	successTrackers := oneTracker{}

	placements, err := nodeselection.LoadConfig(n.PlacementConfig, nodeselection.NewPlacementConfigEnvironment(successTrackers, nil))
	if err != nil {
		return errors.WithStack(err)
	}

	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	satelliteDB, err := satellitedb.Open(ctx, log.Named("metabase"), os.Getenv("STBB_DB_SATELLITE"), satellitedb.Options{
		ApplicationName: "stbb",
	})
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		satelliteDB.Close()
	}()

	cache, err := overlay.NewUploadSelectionCache(log, satelliteDB.OverlayCache(), 60*time.Minute, overlay.NodeSelectionConfig{
		NewNodeFraction:  0.01,
		OnlineWindow:     4 * time.Hour,
		MinimumDiskSpace: 5 * memory.GB,
	}, nil, placements)
	if err != nil {
		return errors.WithStack(err)
	}

	go func() {
		err = cache.Run(ctx)
		fmt.Println(err)
	}()

	err = cache.Refresh(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	selection := placements[n.Placement].EC.Total
	if selection == 0 {
		selection = 110
	}
	success := placements[n.Placement].EC.Success
	if success == 0 {
		success = 65
	}
	k := 100_000
	sum := 0
	stat := map[string]int{}
	oop := map[int]int{}
	for i := 0; i < k; i++ {
		nodes, err := cache.GetNodes(ctx, overlay.FindStorageNodesRequest{
			RequestedCount: selection,
			Placement:      n.Placement,
			Requester:      storj.NodeID{},
		})
		if err != nil {
			return errors.WithStack(err)
		}

		for _, node := range nodes {
			stat[nodeAttribute(*node)]++
			sum++
		}
		pieces, invNodes := convert(nodes)
		inv := placements[storj.PlacementConstraint(n.Placement)].Invariant(pieces, invNodes)
		oop[inv.Count()]++
		rand.Shuffle(len(nodes), func(i, j int) {
			nodes[i], nodes[j] = nodes[j], nodes[i]
		})
		//for ix, node := range nodes {
		//	successTrackers.GetTracker(node.ID).Increment(node.ID, ix < success)
		//}
	}

	output := fmt.Sprintf("I selected %d nodes %d times, and the following groups were used to store pieces:\n", selection, k)

	var groups []string
	for group := range stat {
		groups = append(groups, group)
	}
	sort.Strings(groups)

	for _, group := range groups {
		count := stat[group]
		output += fmt.Sprintf("_%s_: %d %% (%d)\n", group, count*100/sum, count)
	}
	fmt.Println(output)
	for k, c := range oop {
		fmt.Println(k, c)
	}
	fmt.Println()

	//nodes, err := satelliteDB.OverlayCache().GetParticipatingNodes(ctx, 4*time.Hour, 10*time.Millisecond)
	//if err != nil {
	//	return errors.WithStack(err)
	//}
	//for _, node := range nodes {
	//	fmt.Println(node.ID, nodeAttribute(node), successTrackers.GetTracker(node.ID).Get(&node))
	//}

	//min, max := -1, -1
	//hist := map[int]int{}
	//for _, v := range stat {
	//	hist[v]++
	//	if min == -1 || v < min {
	//		min = v
	//	}
	//	if max == -1 || v > max {
	//		max = v
	//	}
	//}
	//for i := min; i <= max; i++ {
	//	if hist[i] > 0 {
	//		fmt.Println(i, hist[i])
	//	}
	//}

	return nil
}

type oneTracker struct {
}

func (o oneTracker) Get(uplink storj.NodeID) func(node *nodeselection.SelectedNode) float64 {
	return func(node *nodeselection.SelectedNode) float64 {
		return 64
	}
}

var _ nodeselection.UploadSuccessTracker = oneTracker{}
