package placement

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"os"
	"sort"
	"storj.io/storj/satellite/nodeselection"
	"storj.io/storj/satellite/overlay"
	"storj.io/storj/satellite/satellitedb"
	"time"
)

type Tags struct {
	ValueTags    []string `usage:"node tags to check the value" default:"hashstore0"`
	CategoryTags []string `usage:"node tags to categorize nodes" default:"server_group,host,service"`
	Filter       string   `usage:"additional display only node filter" default:"tag(\"1111111111111111111111111111111VyS547o\",\"operator\",\"storj.io\") && country(\"US\")"`
}

type TagSet map[string]string

func (s TagSet) Key() string {
	var keys []string
	for k := range s {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var key string
	for _, k := range keys {
		key += k + ":" + s[k] + ","
	}
	return key
}

type NodeGroup struct {
	CategoryTags []TagSet
	ValueTags    TagSet
}

type Simplifications struct {
	Aliases []Alias
}

type Alias struct {
	SimpleTags  TagSet
	ComplexTags []TagSet
}

func (s *Simplifications) Add(simple TagSet, full TagSet) {
	for ix, alias := range s.Aliases {
		if alias.SimpleTags.Key() == simple.Key() {
			s.Aliases[ix].ComplexTags = append(alias.ComplexTags, full)
			return
		}
	}
	s.Aliases = append(s.Aliases, Alias{
		SimpleTags:  simple,
		ComplexTags: []TagSet{full},
	})
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

	reputableNodes, newNodes, err := satelliteDB.OverlayCache().SelectAllStorageNodesUpload(ctx, overlay.NodeSelectionConfig{
		NewNodeFraction: 0.01,
		OnlineWindow:    4 * time.Hour,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	nodes := append(reputableNodes, newNodes...)

	nodeByValues := make(map[string]*NodeGroup)
	simplifications := &Simplifications{}

	for _, node := range nodes {
		valueTags := make(TagSet)
		categoryTags := make(TagSet)
		for _, tag := range node.Tags {
			for _, valueTag := range s.ValueTags {
				if tag.Name == valueTag {
					valueTags[tag.Name] = string(tag.Value)
				}
			}
			for _, categoryTag := range s.CategoryTags {
				if tag.Name == categoryTag {
					categoryTags[tag.Name] = string(tag.Value)
				}
			}
		}

		groupTags := make(TagSet)
		for _, tag := range s.CategoryTags[:len(s.CategoryTags)-1] {
			value, ok := categoryTags[tag]
			if !ok {
				continue
			}
			groupTags[tag] = value

			cx := make(TagSet)
			for k, v := range groupTags {
				cx[k] = v
			}

			simplifications.Add(cx, categoryTags)
		}

		if !nodeFilter.Match(node) {
			continue
		}
		valueKey := valueTags.Key()

		_, ok := nodeByValues[valueKey]
		if !ok {
			nodeByValues[valueKey] = &NodeGroup{
				ValueTags: valueTags,
			}
		}
		nodeByValues[valueKey].CategoryTags = append(nodeByValues[valueKey].CategoryTags, categoryTags)
	}

	//for _, s := range simplifications.Aliases {
	//	fmt.Println("------")
	//	fmt.Println(s.SimpleTags.Key())
	//	fmt.Println("------")
	//	for _, cx := range s.ComplexTags {
	//		fmt.Println("   ", cx.Key())
	//	}
	//	fmt.Println("-===-")
	//}

	for k, _ := range nodeByValues {
		for _, simplification := range simplifications.Aliases {
			if fullyInclude(nodeByValues[k].CategoryTags, simplification.ComplexTags) {
				nodeByValues[k].CategoryTags = simplify(nodeByValues[k].CategoryTags, simplification.ComplexTags, simplification.SimpleTags)
			}
		}

		var filtered []TagSet
		first := true
		for _, tag := range nodeByValues[k].CategoryTags {
			if tag.Key() == "" {
				if first {
					filtered = append(filtered, tag)
					first = false
				}
				continue
			}
			filtered = append(filtered, tag)
		}
		nodeByValues[k].CategoryTags = filtered
	}

	for _, nodes := range nodeByValues {
		fmt.Println("------")
		for k, v := range nodes.ValueTags {
			fmt.Println(k, ":", v, " ")
		}
		fmt.Println("------")
		for _, categoryTags := range nodes.CategoryTags {
			fmt.Println("   ", categoryTags.Key())
		}
	}

	return nil
}

// simplify returns with the base, but all toBeReplaced elements are removed and replacement is added.
func simplify(base []TagSet, toBeReplaced []TagSet, replacement TagSet) []TagSet {
	var result []TagSet
	for _, b := range base {
		found := false
		for _, r := range toBeReplaced {
			if b.Key() == r.Key() {
				found = true
				break
			}
		}
		if !found {
			result = append(result, b)
		}
	}
	result = append(result, replacement)
	return result
}

// fullyInclude returns true if all the signal elements are part of base.
func fullyInclude(base []TagSet, signal []TagSet) bool {
	for _, s := range signal {
		found := false
		for _, b := range base {
			if b.Key() == s.Key() {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
