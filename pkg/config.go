package stbb

import (
	"fmt"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
	"os"
)

func init() {
	cmd := cobra.Command{
		Use: "convert-config",
		RunE: func(cmd *cobra.Command, args []string) error {
			return convertConfig(args[0])
		},
	}

	RootCmd.AddCommand(&cmd)
}

type K8sEntry struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}
type K8sEnv struct {
	Environment []K8sEntry `yaml:"env"`
}

func convertConfig(s string) error {
	raw, err := os.ReadFile(s)
	if err != nil {
		return err
	}
	k := K8sEnv{}
	err = yaml.Unmarshal(raw, &k)
	if err != nil {
		return err
	}
	for _, e := range k.Environment {
		fmt.Println(e.Name, e.Value)
	}
	for _, e := range k.Environment {
		fmt.Printf("<env name=\"%s\" value=\"%s\"/>\n", e.Name, e.Value)
	}

	return nil
}
