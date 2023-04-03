package stbb

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
)

type K8sEntry struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}
type K8sEnv struct {
	Environment []K8sEntry `yaml:"env"`
}

type ConvertConfig struct {
	File string `arg:""`
}

func (c ConvertConfig) Run() error {
	raw, err := os.ReadFile(c.File)
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
