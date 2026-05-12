package config

import (
	"encoding/json"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func LoadJSONConfigFile(filename string) (*Config, error) {
	return loadAndUnmarshal(filename, json.Unmarshal)
}

func LoadYAMLConfigFile(filename string) (*Config, error) {
	return loadAndUnmarshal(filename, yaml.Unmarshal)
}

func loadAndUnmarshal(filename string, unmarshal func([]byte, any) error) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", filename, err)
	}

	var cfg Config
	if err := unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config file %s: %w", filename, err)
	}

	return &cfg, nil
}
