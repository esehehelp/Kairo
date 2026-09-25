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
	AgentToken                 string     `toml:"agent_token"` // enables the node agent API when set
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
		c.DatabasePath = "local/kairo-v3.db"
	}
	if c.Listen == "" {
		c.Listen = "127.0.0.1:7474"
	}
	if c.AdvertiseURL == "" {
		c.AdvertiseURL = "http://127.0.0.1:7474"
	}
	if c.LogDirectory == "" {
		c.LogDirectory = "local/attempts-v3"
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

// Agent configures `kairo agent`: a node on another host whose executors and
// providers are driven through the daemon's /v3/agent API.
type Agent struct {
	// ServerURL is the daemon as reachable from this node (LAN address or an
	// SSH tunnel endpoint); Token is the daemon's agent_token.
	ServerURL string `toml:"server_url"`
	Token     string `toml:"token"`
	// AttemptAPIURL is handed to attempts as KAIRO_API_URL; it defaults to
	// ServerURL.
	AttemptAPIURL              string     `toml:"attempt_api_url"`
	LogDirectory               string     `toml:"log_directory"`
	ObserveOnly                bool       `toml:"observe_only"`
	ObservationIntervalSeconds int        `toml:"observation_interval_seconds"`
	Node                       Node       `toml:"node"`
	Executors                  []Executor `toml:"executors"`
	Providers                  []Provider `toml:"providers"`
}

func LoadAgent(path string) (Agent, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return Agent{}, err
	}
	a := Agent{}
	metadata, err := toml.Decode(string(body), &a)
	if err != nil {
		return a, err
	}
	if undecoded := metadata.Undecoded(); len(undecoded) > 0 {
		return a, errors.New("unknown agent config field: " + undecoded[0].String())
	}
	if a.ServerURL == "" || a.Token == "" {
		return a, errors.New("server_url and token are required")
	}
	if a.AttemptAPIURL == "" {
		a.AttemptAPIURL = a.ServerURL
	}
	if a.LogDirectory == "" {
		a.LogDirectory = "local/attempts-agent"
	}
	if a.ObservationIntervalSeconds == 0 {
		a.ObservationIntervalSeconds = 5
	}
	if a.Node.OS == "" {
		a.Node.OS = runtime.GOOS
	}
	if a.Node.Architecture == "" {
		a.Node.Architecture = runtime.GOARCH
	}
	for i := range a.Providers {
		if a.Providers[i].ObservationTTLSeconds == 0 {
			a.Providers[i].ObservationTTLSeconds = 15
		}
	}
	if a.Node.ID == "" || a.Node.Name == "" {
		return a, errors.New("node.id and node.name are required")
	}
	if len(a.Executors) == 0 {
		return a, errors.New("at least one executor is required")
	}
	return a, nil
}
