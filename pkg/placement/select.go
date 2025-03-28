package placement

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/db"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"sort"
	"storj.io/common/memory"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/overlay"
	"strings"
	"time"
)

type Select struct {
	WithPlacement
	db.WithDatabase
	Placement  int
	NodeNo     int    `default:"110"`
	Selector   string `default:"wallet"`
	Number     int    `default:"1"`
	Durability string `usage:"node attribute to calculate the durability risk for"`
	Invariant  bool   `usage:"Check invariant for all selections"`
}

func (s Select) Run() error {
	ctx := context.Background()

	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}
	d, err := s.WithPlacement.GetPlacement(nodeselection.NewPlacementConfigEnvironment(nil, nil))
	if err != nil {
		return errors.WithStack(err)
	}
	satelliteDB, err := s.WithDatabase.GetSatelliteDB(ctx, log)
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
	}, nil, d)
	if err != nil {
		return errors.WithStack(err)
	}

	go func() {
		err = cache.Run(ctx)
		fmt.Println(err)
	}()

	start := time.Now()
	err = cache.Refresh(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Info("Node cache is loaded", zap.Duration("duration", time.Since(start)))

	var report *durabilityReport
	if s.Durability != "" {
		attr, err := nodeselection.CreateNodeAttribute(s.Durability)
		if err != nil {
			return errors.WithStack(err)
		}
		report = &durabilityReport{
			healthStat: make([]Histogram, 3),
			attribute:  attr,
		}
	}

	stat := map[string]int{}
	var sum int
	oopSelection := 0
	for i := 0; i < s.Number; i++ {
		nodes, err := cache.GetNodes(ctx, overlay.FindStorageNodesRequest{
			RequestedCount: s.NodeNo,
			Placement:      storj.PlacementConstraint(s.Placement),
			Requester:      storj.NodeID{},
		})
		if err != nil {
			return errors.WithStack(err)
		}
		if s.Durability != "" {
			report.Update(nodes)
		} else {
			selector, err := nodeselection.CreateNodeAttribute(s.Selector)
			if err != nil {
				return errors.WithStack(err)
			}
			pieces, invNodes := convert(nodes)
			oop := d[storj.PlacementConstraint(s.Placement)].Invariant(pieces, invNodes)
			if s.Invariant {
				if oop.Count() > 0 {
					oopSelection++
				}
			} else {
				util.PrintHistogram(nodes, selector)
				fmt.Println("Out of placement nodes", oop.Count())
			}
			for _, node := range nodes {
				stat[selector(*node)]++
				sum++
			}
		}
	}
	if s.Invariant {
		fmt.Println("OOP selections", oopSelection)
	}
	if s.Durability != "" {
		for ix, h := range report.healthStat {
			fmt.Println("Report for", s.Durability, ix)
			for ix, b := range h.NegativeBuckets {
				fmt.Println(ix*-1, b.SegmentCount, b.ClassExemplars)
			}
			for ix, b := range h.Buckets {
				fmt.Println(ix, b.SegmentCount, b.ClassExemplars)
			}
		}
	}

	var groups []string
	for group := range stat {
		groups = append(groups, group)
	}
	sort.Strings(groups)

	for _, group := range groups {
		count := stat[group]
		fmt.Printf("_%s_: %d %% (%d)\n", group, count*100/sum, count)
	}

	return nil
}

func convert(orig []*nodeselection.SelectedNode) (pieces metabase.Pieces, nodes []nodeselection.SelectedNode) {
	for ix, node := range orig {
		pieces = append(pieces, metabase.Piece{
			Number: uint16(ix),
		})
		nodes = append(nodes, *node)
	}
	return
}

type durabilityReport struct {
	attribute  nodeselection.NodeAttribute
	healthStat []Histogram
}

func (d *durabilityReport) Update(nodes []*nodeselection.SelectedNode) {
	counters := map[string]int{}

	for _, node := range nodes {
		class := d.attribute(*node)
		counters[class]++

	}
	healthyPieces := len(nodes)

	for i := 0; i < len(d.healthStat); i++ {
		var maxClass string
		maxCount := 0

		for group, counter := range counters {
			if counter > maxCount {
				maxCount = counter
				maxClass = group
			}
		}
		d.healthStat[i].AddPieceCount(healthyPieces, maxClass)

		healthyPieces -= maxCount
		counters[maxClass] = 0
	}
}

func debugNodes(nodes []*nodeselection.SelectedNode, attribute nodeselection.NodeAttribute) string {
	var res []string
	for _, node := range nodes {
		res = append(res, attribute(*node))
	}
	return strings.Join(res, ",")
}

type Histogram struct {
	// pieceCount -> {number of segments, exemplars}
	Buckets []*Bucket
	// pieceCount * -1 -> {number of segments, exemplars}
	NegativeBuckets []*Bucket
}

// Bucket stores the number of segments (and some exemplars) for each piece count.
type Bucket struct {
	SegmentCount   int
	ClassExemplars []string
}

const maxExemplars = 1

// Increment increments the bucket counters.
func (b *Bucket) Increment(classExemplar string) {
	b.SegmentCount++
	if len(b.ClassExemplars) < maxExemplars {
		b.ClassExemplars = append(b.ClassExemplars, classExemplar)
	}
}

// Reset resets the bucket counters.
func (b *Bucket) Reset() {
	b.SegmentCount = 0
	b.ClassExemplars = b.ClassExemplars[:0]
}

// AddPieceCount adds a piece count to the histogram.
func (h *Histogram) AddPieceCount(pieceCount int, classExemplar string) {
	if pieceCount < 0 {
		for len(h.NegativeBuckets) <= -pieceCount {
			h.NegativeBuckets = append(h.NegativeBuckets, &Bucket{})
		}
		h.NegativeBuckets[-pieceCount].Increment(classExemplar)
	} else {
		for len(h.Buckets) <= pieceCount {
			h.Buckets = append(h.Buckets, &Bucket{})
		}
		h.Buckets[pieceCount].Increment(classExemplar)
	}
}

// Reset resets the histogram counters.
func (h *Histogram) Reset() {
	for _, b := range h.Buckets {
		b.Reset()
	}
	for _, b := range h.NegativeBuckets {
		b.Reset()
	}
}
