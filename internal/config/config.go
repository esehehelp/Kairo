package config

import (
	"errors"
	"os"
	"runtime"

	"github.com/BurntSushi/toml"
)

type Config struct {
	DatabasePath               string     `toml:"database_path"`
	Listen                     string     `toml:"listen"`
	AdvertiseURL               string     `toml:"advertise_url"`
	LogDirectory               string     `toml:"log_directory"`
	ObserveOnly                bool       `toml:"observe_only"`
	ObservationIntervalSeconds int        `toml:"observation_interval_seconds"`
	Node                       Node       `toml:"node"`
	Executors                  []Executor `toml:"executors"`
	Providers                  []Provider `toml:"providers"`
}
type Node struct {
	ID           string            `toml:"id"`
	Name         string            `toml:"name"`
	OS           string            `toml:"os"`
	Architecture string            `toml:"architecture"`
	Labels       map[string]string `toml:"labels"`
}
type Executor struct {
	ID      string            `toml:"id"`
	Kind    string            `toml:"kind"`
	Labels  map[string]string `toml:"labels"`
	Enabled *bool             `toml:"enabled"`
}
type Provider struct {
	ID                    string   `toml:"id"`
	Kind                  string   `toml:"kind"`
	Filesystems           []string `toml:"filesystems"`
	ObservationTTLSeconds int      `toml:"observation_ttl_seconds"`
}

func Load(path string) (Config, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	c := Config{}
	metadata, err := toml.Decode(string(body), &c)
	if err != nil {
		return c, err
	}
	if undecoded := metadata.Undecoded(); len(undecoded) > 0 {
		return c, errors.New("unknown config field: " + undecoded[0].String())
	}
	defaults(&c)
	if c.Node.ID == "" || c.Node.Name == "" {
		return c, errors.New("node.id and node.name are required")
	}
	if len(c.Executors) == 0 {
		return c, errors.New("at least one executor is required")
	}
	return c, nil
}
func defaults(c *Config) {
	if c.DatabasePath == "" {
		c.DatabasePath = "local/kairo.db"
	}
	if c.Listen == "" {
		c.Listen = "127.0.0.1:7474"
	}
	if c.AdvertiseURL == "" {
		c.AdvertiseURL = "http://127.0.0.1:7474"
	}
	if c.LogDirectory == "" {
		c.LogDirectory = "local/attempts"
	}
	if c.ObservationIntervalSeconds == 0 {
		c.ObservationIntervalSeconds = 5
	}
	if c.Node.OS == "" {
		c.Node.OS = runtime.GOOS
	}
	if c.Node.Architecture == "" {
		c.Node.Architecture = runtime.GOARCH
	}
	for i := range c.Providers {
		if c.Providers[i].ObservationTTLSeconds == 0 {
			c.Providers[i].ObservationTTLSeconds = 15
		}
	}
}
