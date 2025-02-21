package placement

import (
	"context"
	"fmt"
	"github.com/jtolio/mito"
	"github.com/pkg/errors"
	"reflect"
	"sort"
	"storj.io/storj/satellite/nodeselection"
	"strings"

	"go.uber.org/zap"
	"os"
	"storj.io/common/storj"

	"storj.io/storj/satellite/satellitedb"
	"time"
)

type Score struct {
	Placement storj.PlacementConstraint
	Filter    string `default:"tag(\"1111111111111111111111111111111VyS547o\",\"soc2\",\"true\")"`
	Score     string `default:"node_value(\"free_disk\")"`
}

func (n *Score) Run() (err error) {
	ctx := context.Background()

	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	of := &oneTracker{}

	satelliteDB, err := satellitedb.Open(ctx, log.Named("metabase"), os.Getenv("STBB_DB_SATELLITE"), satellitedb.Options{
		ApplicationName: "stbb",
	})
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		satelliteDB.Close()
	}()

	filter, err := nodeselection.FilterFromString(n.Filter, &nodeselection.PlacementConfigEnvironment{})
	if err != nil {
		return errors.WithStack(err)
	}
	nodes, err := satelliteDB.OverlayCache().GetParticipatingNodes(ctx, 4*time.Hour, -1*time.Second)
	if err != nil {
		return errors.WithStack(err)
	}

	env := map[any]any{
		"node_attribute": nodeselection.CreateNodeAttribute,
		"node_value":     nodeselection.CreateNodeValue,
	}
	nodeselection.AddArithmetic(env)
	score, err := mito.Eval(n.Score, env)
	if err != nil {
		return errors.WithStack(err)
	}
	sc, err := nodeselection.ConvertType(score, reflect.TypeOf(new(nodeselection.ScoreNode)).Elem())
	if err != nil {
		return errors.WithStack(err)
	}

	var attributes []nodeselection.NodeAttribute
	for _, t := range []string{"tag:server_group", "tag:host", "tag:service"} {
		attribute, err := nodeselection.CreateNodeAttribute(t)
		if err != nil {
			return errors.WithStack(err)
		}
		attributes = append(attributes, attribute)
	}
	sort.Slice(nodes, func(i, j int) bool {
		s1 := sc.(nodeselection.ScoreNode).Get(storj.NodeID{})(&nodes[i])
		s2 := sc.(nodeselection.ScoreNode).Get(storj.NodeID{})(&nodes[j])
		return s1 < s2
	})
	for _, n := range nodes {
		if filter.Match(&n) {
			fmt.Println(n.ID, of.Get(storj.NodeID{})(&n), sc.(nodeselection.ScoreNode).Get(storj.NodeID{})(&n), tags(n, attributes))
		}
	}

	return nil
}

func tags(n nodeselection.SelectedNode, attributes []nodeselection.NodeAttribute) string {
	var res []string
	for _, a := range attributes {
		res = append(res, a(n))
	}
	return strings.Join(res, ",")
}
