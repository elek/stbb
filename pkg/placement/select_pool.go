package placement

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/elek/stbb/pkg/db"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"storj.io/common/testrand"
	"storj.io/storj/satellite/metainfo"

	"sort"

	"go.uber.org/zap"
	"storj.io/common/memory"
	"storj.io/common/storj"

	"strings"
	"time"

	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/overlay"
)

type SelectPool struct {
	WithPlacement
	db.WithDatabase
	Placement storj.PlacementConstraint
	Selector  string
	CSV       bool
	Tracker   string `default:"noop"`
	Rps       int    `default:"400"`
	K         int    `default:"1000000"`
	FakeNodes int    `default:"0" help:"Number of fake nodes to use instead of db"`
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

	placements, err := n.WithPlacement.GetPlacement(nodeselection.NewPlacementConfigEnvironment(tw.InitScoreNode(), &NoopFailureTracker{}))
	if err != nil {
		return errors.WithStack(err)
	}

	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	var nodeSource overlay.UploadSelectionDB
	if n.FakeNodes > 0 {
		zero, _ := storj.NodeIDFromString("1111111111111111111111111111111VyS547o")
		var nodes []*nodeselection.SelectedNode
		for i := 0; i < n.FakeNodes; i++ {
			id := testrand.NodeID()
			nodes = append(nodes, &nodeselection.SelectedNode{
				ID: id,
				Tags: nodeselection.NodeTags{
					{
						NodeID: id,
						Signer: zero,
						Name:   "soc2",
						Value:  []byte("true"),
					},
					{
						NodeID: id,
						Signer: zero,
						Name:   "operator",
						Value:  []byte("storj.io"),
					},
					{
						NodeID: id,
						Signer: zero,
						Name:   "server_group",
						Value:  []byte(fmt.Sprintf("group%d", i%5)),
					},
					{
						NodeID: id,
						Signer: zero,
						Name:   "host",
						Value:  []byte(fmt.Sprintf("group%d", i/5/36)),
					},
					{
						NodeID: id,
						Signer: zero,
						Name:   "service",
						Value:  []byte(fmt.Sprintf("instance%d", i)),
					},
				},
			})
		}
		nodeSource = &NodeList{Nodes: nodes}

	} else {
		satelliteDB, err := n.WithDatabase.GetSatelliteDB(ctx, log)
		if err != nil {
			return errors.WithStack(err)
		}
		defer func() {
			satelliteDB.Close()
		}()
		nodeSource = satelliteDB.OverlayCache()
	}

	cache, err := overlay.NewUploadSelectionCache(log, nodeSource, 60*time.Minute, overlay.NodeSelectionConfig{
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
	success := 65
	if placements[n.Placement].EC.Success != nil {
		success = placements[n.Placement].EC.Success(placements[n.Placement].EC.Minimum)
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
		output += "value\n"
	}

	var groups []string
	for group := range stat {
		groups = append(groups, group)
	}
	sort.Strings(groups)

	for _, group := range groups {
		count := stat[group]

		if n.CSV {
			output += fmt.Sprintf("%s,%d\n", group, count)
		} else {
			output += fmt.Sprintf("_%s_: %d %% (%d)\n", group, count*100/sum, count)
		}
	}
	fmt.Println(output)
	if !n.CSV {
		keys := maps.Keys(oop)
		sort.Ints(keys)
		for k := range keys {
			fmt.Println(k, oop[k])
		}
		fmt.Println()
	}

	//tw.Debug()
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
	Debug()
}

type Noop struct {
}

func (n *Noop) Debug() {
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

func (f *Fair) Debug() {
	for id, score := range f.tracker.counters {
		fmt.Println(id, score)
	}
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

func (b *BitShift) Debug() {

}

func (b *BitShift) InitScoreNode() nodeselection.ScoreNode {
	tracker, ok := metainfo.GetNewSuccessTracker("bitshift")
	if !ok {
		panic("unknown tracker")
	}
	successTracker := metainfo.NewSuccessTrackers([]storj.NodeID{}, func(id storj.NodeID) metainfo.SuccessTracker {
		return tracker()
	})
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

type NodeList struct {
	Nodes []*nodeselection.SelectedNode
}

func (n NodeList) SelectAllStorageNodesUpload(ctx context.Context, selectionCfg overlay.NodeSelectionConfig) (reputable, new []*nodeselection.SelectedNode, err error) {
	return n.Nodes, nil, nil
}

type NoopFailureTracker struct {
}

func (n *NoopFailureTracker) Get(node *nodeselection.SelectedNode) float64 {
	return 1
}

var _ nodeselection.UploadFailureTracker = &NoopFailureTracker{}
