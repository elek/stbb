package config

import (
	"errors"
	"fmt"
	"github.com/alecthomas/kong"
	"gopkg.in/yaml.v3"
	"io"
)

// Loader is a Kong configuration loader for YAML.
func Loader(r io.Reader) (kong.Resolver, error) {
	decoder := yaml.NewDecoder(r)
	config := map[string]interface{}{}
	err := decoder.Decode(config)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("YAML config decode error: %w", err)
	}
	return kong.ResolverFunc(func(context *kong.Context, parent *kong.Path, flag *kong.Flag) (interface{}, error) {
		//// Build a string path up to this flag.
		path := []string{flag.Name}
		val, found := find(config, path)
		if found {
			return val, nil
		}
		for n := parent.Node(); n != nil && n.Type != kong.ApplicationNode; n = n.Parent {
			path = append([]string{n.Name}, path...)
			val, found := find(config, path)
			if found {
				return val, nil
			}
		}
		return nil, nil
	}), nil
}

func find(config map[string]interface{}, path []string) (interface{}, bool) {
	if len(path) == 0 {
		return config, true
	}
	for i := 0; i < len(path); i++ {
		if child, ok := config[path[0]].(map[string]interface{}); ok {
			return find(child, path[i+1:])
		}
	}
	return config[path[0]], true
}
