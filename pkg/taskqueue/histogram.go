package taskqueue

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"github.com/elek/stbb/pkg/db"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"storj.io/common/storj"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/overlay"
)

type nodeStats struct {
	nodeID storj.NodeID
	count  int
	tags   string
}

// nodeHistogram reads all jobs from the given Redis stream, groups them by
// the node returned by nodeSelector (either SourceNode or DestNode), resolves
// node tags from the satellite DB, and prints a histogram sorted by count.
func nodeHistogram(cfg histogramConfig) error {
	ctx := context.Background()
	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	redisOpts, err := redis.ParseURL(cfg.Address)
	if err != nil {
		return errors.WithStack(err)
	}
	redisOpts.ReadTimeout = 5 * time.Minute
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()

	counts, total, err := countNodes(ctx, rdb, cfg.Stream, cfg.nodeSelector)
	if err != nil {
		return err
	}

	if total == 0 {
		fmt.Println("No jobs in stream", cfg.Stream)
		return nil
	}

	// Load all upload nodes (with tags) in a single query.
	satelliteDB, err := cfg.WithDatabase.GetSatelliteDB(ctx, log)
	if err != nil {
		return err
	}
	defer satelliteDB.Close()

	nodeMap, err := loadNodeMap(ctx, satelliteDB.OverlayCache())
	if err != nil {
		return err
	}

	var attributes []nodeselection.NodeAttribute
	for _, a := range cfg.Attributes {
		attr, err := nodeselection.CreateNodeAttribute(a)
		if err != nil {
			return errors.WithStack(err)
		}
		attributes = append(attributes, attr)
	}

	var results []nodeStats
	for nodeID, count := range counts {
		tagStr := formatNodeAttrs(nodeMap, nodeID, attributes)
		results = append(results, nodeStats{
			nodeID: nodeID,
			count:  count,
			tags:   tagStr,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].count > results[j].count
	})

	for _, r := range results {
		if r.tags != "" {
			fmt.Printf("%6d %s %s\n", r.count, r.nodeID, r.tags)
		} else {
			fmt.Printf("%6d %s\n", r.count, r.nodeID)
		}
	}

	fmt.Printf("\nTotal: %d jobs across %d unique nodes\n", total, len(counts))
	return nil
}

// loadNodeMap loads all upload-eligible nodes (with tags already populated)
// into a map keyed by NodeID.
func loadNodeMap(ctx context.Context, cache overlay.DB) (map[storj.NodeID]*nodeselection.SelectedNode, error) {
	reputable, new, err := cache.SelectAllStorageNodesUpload(ctx, overlay.NodeSelectionConfig{
		OnlineWindow: 168 * time.Hour,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	nodeMap := make(map[storj.NodeID]*nodeselection.SelectedNode, len(reputable)+len(new))
	for _, n := range reputable {
		nodeMap[n.ID] = n
	}
	for _, n := range new {
		nodeMap[n.ID] = n
	}
	return nodeMap, nil
}

func formatNodeAttrs(nodeMap map[storj.NodeID]*nodeselection.SelectedNode, nodeID storj.NodeID, attributes []nodeselection.NodeAttribute) string {
	if len(attributes) == 0 {
		return ""
	}

	node, ok := nodeMap[nodeID]
	if !ok {
		return "(unknown)"
	}

	result := ""
	for i, attr := range attributes {
		if i > 0 {
			result += " "
		}
		result += attr(*node)
	}
	return result
}

// countNodes reads all jobs from the Redis stream in batches, counting
// occurrences of the selected node without storing every job in memory.
func countNodes(ctx context.Context, rdb *redis.Client, stream string, selector func(storj.NodeID, storj.NodeID) storj.NodeID) (map[storj.NodeID]int, int, error) {
	counts := map[storj.NodeID]int{}
	total := 0
	start := "-"
	for {
		msgs, err := rdb.XRangeN(ctx, stream, start, "+", 5000).Result()
		if err != nil {
			return nil, 0, errors.WithStack(err)
		}
		if len(msgs) == 0 {
			break
		}
		for _, msg := range msgs {
			src, dst, err := decodeNodes(msg.Values)
			if err != nil {
				return nil, 0, errors.Wrap(err, "decoding job "+msg.ID)
			}
			counts[selector(src, dst)]++
			total++
		}
		lastID := msgs[len(msgs)-1].ID
		start = "(" + lastID
		if total%100000 == 0 {
			fmt.Printf("\rReading... %d jobs", total)
		}
	}
	if total >= 100000 {
		fmt.Printf("\rReading... %d jobs\n", total)
	}
	return counts, total, nil
}

func decodeNodes(values map[string]interface{}) (storj.NodeID, storj.NodeID, error) {
	src, err := decodeNodeID(values, "source_node")
	if err != nil {
		return storj.NodeID{}, storj.NodeID{}, err
	}
	dst, err := decodeNodeID(values, "dest_node")
	if err != nil {
		return storj.NodeID{}, storj.NodeID{}, err
	}
	return src, dst, nil
}

func decodeNodeID(values map[string]interface{}, key string) (storj.NodeID, error) {
	raw, ok := values[key]
	if !ok {
		return storj.NodeID{}, fmt.Errorf("missing field %q", key)
	}
	s, ok := raw.(string)
	if !ok {
		return storj.NodeID{}, fmt.Errorf("field %q: expected string, got %T", key, raw)
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return storj.NodeID{}, errors.Wrapf(err, "field %q", key)
	}
	return storj.NodeIDFromBytes(b)
}

type histogramConfig struct {
	db.WithDatabase
	Address      string
	Stream       string
	Attributes   []string
	nodeSelector func(storj.NodeID, storj.NodeID) storj.NodeID
}
