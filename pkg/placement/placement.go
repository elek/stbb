package placement

import (
	"os"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
	"storj.io/storj/satellite/nodeselection"
)

type Placement struct {
	Select       Select       `cmd:"" help:"select given number of nodes from DB, matching the placement rule"`
	SelectPool   SelectPool   `cmd:"" help:"select given number of nodes from DB, printing simplified pool stat"`
	Nodes        Nodes        `cmd:"" help:"load the upload cache and print out statistics"`
	List         List         `cmd:"" help:"list nodes available for selection"`
	Tags         Tags         `cmd:"" help:"report current tag distribution"`
	QueryTags    QueryTags    `cmd:"" help:"generate query for tags"`
	Simulate     Simulate     `cmd:"" help:"selection simulation with histogram"`
	Score        Score        `cmd:"" help:"print out node scores"`
	Replicasets  Replicasets  `cmd:"" help:"experiments with replicasets"`
	DownloadPool DownloadPool `cmd:"" help:"test download pool, with requesting downloads from satellite and classify received nodes"`
	Download     Download     `cmd:"" help:"initiate a download request and print out the selected nodes (using satellite)"`
}

type WithPlacement struct {
	PlacementConfig string `help:"location of the placement file" yaml:"placement-config"`
}

func (w WithPlacement) GetPlacement(environment nodeselection.PlacementConfigEnvironment) (nodeselection.PlacementDefinitions, error) {
	content, err := os.ReadFile(w.PlacementConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	m := map[string]interface{}{}
	err = yaml.Unmarshal(content, &m)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	placementDef, wrapped := m["placement"]
	if !wrapped {
		placementDef = string(content)
	}

	return nodeselection.LoadConfigFromString(placementDef.(string), environment)
}
