package placement

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/db"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"slices"
	"storj.io/storj/satellite/nodeselection"
	"strings"
	"time"
)

type Tags struct {
	db.WithDatabase
	ValueTags   []string `usage:"node tags to check the value" default:""`
	CategoryTag string   `usage:"node tags to categorize nodes" default:"tag:server_group"`
	Filter      string   `usage:"additional display only node filter" default:""`
	All         bool
}

func (s Tags) Run() error {
	ctx := context.Background()

	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	nodeFilter, err := nodeselection.FilterFromString(s.Filter, nil)
	if err != nil {
		return err
	}

	satelliteDB, err := s.WithDatabase.GetSatelliteDB(ctx, log)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		satelliteDB.Close()
	}()

	nodes, err := satelliteDB.OverlayCache().GetAllParticipatingNodes(ctx, 4*time.Hour, 10*time.Millisecond)
	if err != nil {
		return errors.WithStack(err)
	}

	categoryAttribute, err := nodeselection.CreateNodeAttribute(s.CategoryTag)
	if err != nil {
		return errors.WithStack(err)
	}

	valueAttributes := make(map[string]nodeselection.NodeAttribute)
	for _, attr := range s.ValueTags {
		n, err := nodeselection.CreateNodeAttribute(attr)
		if err != nil {
			return err
		}
		valueAttributes[attr] = n
	}

	// category --> key --> value --> count
	values := make(map[string]map[string]map[string]int)
	for _, node := range nodes {
		if !nodeFilter.Match(&node) {
			continue
		}

		category := categoryAttribute(node)
		if _, ok := values[category]; !ok {
			values[category] = map[string]map[string]int{}
			values[category]["all"] = map[string]int{}
		}
		values[category]["all"]["count"]++

		if s.All {
			for _, tag := range node.Tags {
				if _, ok := values[category][tag.Name]; !ok {
					values[category][tag.Name] = map[string]int{}
				}

				values[category][tag.Name][string(tag.Value)]++
			}
		} else {
			for name, v := range valueAttributes {
				value := v(node)
				if _, ok := values[category][name]; !ok {
					values[category][name] = map[string]int{}
				}

				values[category][name][value]++
			}
		}

	}

	keys := maps.Keys(values)
	slices.Sort(keys)
	for _, k := range keys {

		group := k
		if group == "" {
			group = "<untagged>"
		}
		count := values[k]["all"]["count"]
		fmt.Printf("%s (%d instances)\n", group, count)

		nameKeys := maps.Keys(values[k])
		slices.Sort(nameKeys)
		for _, name := range nameKeys {
			if name == "all" {
				continue
			}
			valueKeys := maps.Keys(values[k][name])
			slices.Sort(valueKeys)
			var valueSummary []string
			for _, value := range valueKeys {
				if values[k][name][value] == 0 || value == "" {
					continue
				}
				valueSummary = append(valueSummary, fmt.Sprintf("=%s (%d instances)", value, values[k][name][value]))
			}
			if len(valueSummary) > 0 {
				fmt.Println("      ", name, strings.Join(valueSummary, ", "))
			}
		}
	}
	return nil
}
