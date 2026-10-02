package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"runtime"

	"github.com/BurntSushi/toml"
)

type Config struct {
	DatabasePath               string `toml:"database_path"`
	Listen                     string `toml:"listen"`
	AdvertiseURL               string `toml:"advertise_url"`
	LogDirectory               string `toml:"log_directory"`
	ObserveOnly                bool   `toml:"observe_only"`
	ObservationIntervalSeconds int    `toml:"observation_interval_seconds"`
	// TLS: the daemon serves HTTPS with this certificate (kairo tls init).
	// The CA is handed to attempts so they can verify the daemon.
	TLSCertFile string `toml:"tls_cert_file"`
	TLSKeyFile  string `toml:"tls_key_file"`
	TLSCAFile   string `toml:"tls_ca_file"`
	// InsecureHTTP allows plain HTTP on an address other hosts can reach.
	InsecureHTTP bool       `toml:"insecure_http"`
	Node         Node       `toml:"node"`
	Executors    []Executor `toml:"executors"`
	Providers    []Provider `toml:"providers"`
}

// TLS reports whether the daemon serves HTTPS.
func (c Config) TLS() bool { return c.TLSCertFile != "" || c.TLSKeyFile != "" || c.TLSCAFile != "" }

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
	return c, validateTransport(c)
}

// validateTransport refuses a daemon that would take bearer tokens in plain
// HTTP from other hosts unless that is asked for explicitly.
func validateTransport(c Config) error {
	if c.TLS() {
		if c.TLSCertFile == "" || c.TLSKeyFile == "" || c.TLSCAFile == "" {
			return errors.New("tls_cert_file, tls_key_file and tls_ca_file go together")
		}
		if c.InsecureHTTP {
			return errors.New("insecure_http contradicts the tls_* settings")
		}
		if scheme(c.AdvertiseURL) != "https" {
			return fmt.Errorf("advertise_url %q must be https when the daemon serves TLS", c.AdvertiseURL)
		}
		return nil
	}
	if scheme(c.AdvertiseURL) != "http" {
		return fmt.Errorf("advertise_url %q must be http without tls_* settings", c.AdvertiseURL)
	}
	if !loopback(c.Listen) && !c.InsecureHTTP {
		return fmt.Errorf("listen %q is reachable from other hosts: configure tls_cert_file, tls_key_file and tls_ca_file (kairo tls init), or set insecure_http = true", c.Listen)
	}
	return nil
}

func scheme(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Scheme
}

// loopback reports whether a listen address only accepts local connections.
func loopback(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
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
		if c.TLS() {
			c.AdvertiseURL = "https://127.0.0.1:7474"
		}
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
// providers are driven through the daemon's /api/agent operations.
type Agent struct {
	// ServerURL is the daemon as reachable from this node (LAN address or an
	// SSH tunnel endpoint); Token is this node's token (kairo token create
	// --role node --node NODE_ID).
	ServerURL string `toml:"server_url"`
	Token     string `toml:"token"`
	// AttemptAPIURL is handed to attempts as KAIRO_API_URL; it defaults to
	// ServerURL.
	AttemptAPIURL string `toml:"attempt_api_url"`
	// CAFile is the daemon's CA certificate (ca.pem from kairo tls init); it
	// is also handed to this node's attempts. Empty: system roots.
	CAFile string `toml:"ca_file"`
	// InsecureHTTP allows an http:// server_url.
	InsecureHTTP               bool       `toml:"insecure_http"`
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
	for _, u := range []string{a.ServerURL, a.AttemptAPIURL} {
		switch scheme(u) {
		case "https":
		case "http":
			if !a.InsecureHTTP {
				return a, fmt.Errorf("%q is plain HTTP: the node token would travel unencrypted; use https or set insecure_http = true", u)
			}
		default:
			return a, fmt.Errorf("%q is not an http(s) URL", u)
		}
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
