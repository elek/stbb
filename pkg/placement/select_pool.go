package placement

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"math/rand"
	"storj.io/storj/satellite/metainfo"

	"go.uber.org/zap"
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
	CSV             bool
	Tracker         string `default:"noop"`
	Rps             int    `default:"400"`
	K               int    `default:"1000000"`
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

	var tw TrackerWrap
	switch n.Tracker {
	case "noop":
		tw = &Noop{}
	case "fair":
		tw = &Fair{}
	case "bitshift":
		tw = &BitShift{}
	default:
		return errors.New("unknown tracker: " + n.Tracker)
	}
	placements, err := nodeselection.LoadConfig(n.PlacementConfig, nodeselection.NewPlacementConfigEnvironment(tw.InitScoreNode(), nil))
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

	sum := 0
	stat := map[string]int{}
	oop := map[int]int{}
	for i := 0; i < n.K; i++ {
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
		inv := placements[n.Placement].Invariant(pieces, invNodes)
		oop[inv.Count()]++

		tw.Increment(nodes, success)

		if i%n.Rps == 0 {
			tw.Bump()
		}
	}

	var output string
	if !n.CSV {
		output = fmt.Sprintf("I selected %d nodes %d times, and the following groups were used to store pieces:\n", selection, n.K)
	} else {

		for _, s := range strings.Split(selectorDef, ",") {
			parts := strings.Split(s, ":")
			output += fmt.Sprintf("%s,", parts[len(parts)-1])
		}
		output += "percentage,selection\n"
	}

	var groups []string
	for group := range stat {
		groups = append(groups, group)
	}
	sort.Strings(groups)

	for _, group := range groups {
		count := stat[group]

		if n.CSV {
			output += fmt.Sprintf("_%s_,%d,%d\n", group, count*100/sum, count)
		} else {
			output += fmt.Sprintf("_%s_: %d %% (%d)\n", group, count*100/sum, count)
		}
	}
	fmt.Println(output)
	if !n.CSV {
		for k, c := range oop {
			fmt.Println(k, c)
		}
		fmt.Println()
	}

	//nodes, err := satelliteDB.OverlayCache().GetParticipatingNodes(ctx, 4*time.Hour, 10*time.Millisecond)
	//if err != nil {
	//	return errors.WithStack(err)
	//}
	//for _, node := range nodes {
	//	fmt.Println(node.ID, nodeAttribute(node), successTracker.InitScoreNode(storj.NodeID{}).Get(&node))
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

type TrackerWrap interface {
	Increment(nodes []*nodeselection.SelectedNode, success int)
	Bump()
	InitScoreNode() nodeselection.ScoreNode
}

type Noop struct {
}

func (n *Noop) Increment(nodes []*nodeselection.SelectedNode, success int) {
	return
}

func (n *Noop) Bump() {

}

func (n *Noop) InitScoreNode() nodeselection.ScoreNode {
	return &oneTracker{}
}

var _ TrackerWrap = &Noop{}

type Fair struct {
	tracker *FairTracker
}

func (f *Fair) Increment(nodes []*nodeselection.SelectedNode, success int) {
	for _, node := range nodes {
		f.tracker.Update(node)
	}
}

func (f *Fair) Bump() {
	f.tracker.BumpGeneration()
}

func (f *Fair) InitScoreNode() nodeselection.ScoreNode {
	f.tracker = NewFairTracker()
	return f.tracker
}

var _ TrackerWrap = &Fair{}

type BitShift struct {
	tracker *metainfo.SuccessTrackers
}

func (b *BitShift) InitScoreNode() nodeselection.ScoreNode {
	tracker, ok := metainfo.GetNewSuccessTracker("bitshift")
	if !ok {
		panic("unknown tracker")
	}
	successTracker := metainfo.NewSuccessTrackers([]storj.NodeID{}, tracker)
	b.tracker = successTracker
	return successTracker
}

func (b *BitShift) Increment(nodes []*nodeselection.SelectedNode, success int) {
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})

	for ix, node := range nodes {
		b.tracker.GetTracker(storj.NodeID{}).Increment(node.ID, ix < success)
	}
}

func (b *BitShift) Bump() {
	b.tracker.BumpGeneration()
}

var _ TrackerWrap = &BitShift{}
