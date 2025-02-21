package placement

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"os"
	"slices"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/satellitedb"
	"strings"
	"time"
)

type Tags struct {
	ValueTags   []string `usage:"node tags to check the value" default:"tag:vivint-exclude-upload,tag:surge,tag:us-select-exclude-upload,tag:soc2,tag:owner,tag:weight"`
	CategoryTag string   `usage:"node tags to categorize nodes" default:"tag:server_group"`
	Filter      string   `usage:"additional display only node filter" default:"tag(\"1111111111111111111111111111111VyS547o\",\"operator\",\"storj.io\") && country(\"US\")"`
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

	satelliteDB, err := satellitedb.Open(ctx, log.Named("metabase"), os.Getenv("STBB_DB_SATELLITE"), satellitedb.Options{
		ApplicationName: "stbb",
	})
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		satelliteDB.Close()
	}()

	nodes, err := satelliteDB.OverlayCache().GetParticipatingNodes(ctx, 4*time.Hour, 10*time.Millisecond)
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

		for name, v := range valueAttributes {
			value := v(node)
			if _, ok := values[category][name]; !ok {
				values[category][name] = map[string]int{}
			}

			values[category][name][value]++
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
