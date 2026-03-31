package taskqueue

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/redis/go-redis/v9"
	"github.com/zeebo/errs"

	"storj.io/common/storj"
)

// Stat reads all entries from a Redis stream and prints per source_node->dest_node pair counts.
type Stat struct {
	Address  string `help:"redis URL for task queue" default:"redis://localhost:6379"`
	StreamID string `help:"Redis stream name" default:"balancer"`
}

type pair struct {
	Source storj.NodeID
	Dest   storj.NodeID
}

func (s Stat) Run() error {
	ctx := context.Background()

	opts, err := redis.ParseURL(s.Address)
	if err != nil {
		return errs.New("invalid Redis URL: %v", err)
	}

	db := redis.NewClient(opts)
	defer db.Close()

	counts := map[pair]int{}
	total := 0
	lastID := "-"
	for {
		msgs, err := db.XRangeN(ctx, s.StreamID, lastID, "+", 10000).Result()
		if err != nil {
			return errs.Wrap(err)
		}
		if len(msgs) == 0 {
			break
		}

		for _, msg := range msgs {
			if msg.ID == lastID {
				continue
			}
			total++

			sourceHex, _ := msg.Values["source_node"].(string)
			destHex, _ := msg.Values["dest_node"].(string)

			sourceBytes, err := hex.DecodeString(sourceHex)
			if err != nil {
				return errs.New("invalid source_node hex %q: %v", sourceHex, err)
			}
			destBytes, err := hex.DecodeString(destHex)
			if err != nil {
				return errs.New("invalid dest_node hex %q: %v", destHex, err)
			}

			sourceID, err := storj.NodeIDFromBytes(sourceBytes)
			if err != nil {
				return errs.Wrap(err)
			}
			destID, err := storj.NodeIDFromBytes(destBytes)
			if err != nil {
				return errs.Wrap(err)
			}

			counts[pair{Source: sourceID, Dest: destID}]++
		}
		lastID = msgs[len(msgs)-1].ID

		if len(msgs) < 10000 {
			break
		}
	}

	type result struct {
		p     pair
		count int
	}
	var results []result
	for p, c := range counts {
		results = append(results, result{p: p, count: c})
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].count > results[j].count
	})

	fmt.Printf("%-40s %-40s %s\n", "SOURCE", "DEST", "COUNT")
	for _, r := range results {
		fmt.Printf("%-40s %-40s %d\n", r.p.Source, r.p.Dest, r.count)
	}
	fmt.Printf("\nTotal pairs: %d\n", len(results))
	fmt.Printf("Total entries: %d\n", total)

	return nil
}
