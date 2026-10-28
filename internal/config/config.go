package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/BurntSushi/toml"
)

type Config struct {
	// DatabaseURL is the PostgreSQL database (a postgres:// URL or a
	// key=value connection string); KAIRO_DATABASE_URL may give it instead.
	DatabaseURL string `toml:"database_url"`
	// DatabasePasswordFile holds the database password (its first line),
	// for a DatabaseURL without one.
	DatabasePasswordFile string `toml:"database_password_file"`
	Listen               string `toml:"listen"`
	AdvertiseURL         string `toml:"advertise_url"`
	LogDirectory string `toml:"log_directory"`
	// LogMinFreeMiB is the free space (MiB) the filesystem holding
	// log_directory must have for an executor to launch (0: 1024).
	LogMinFreeMiB              int  `toml:"log_min_free_mib"`
	ObserveOnly                bool `toml:"observe_only"`
	ObservationIntervalSeconds int  `toml:"observation_interval_seconds"`
	// TLS: the daemon serves HTTPS with this certificate (kairo tls init).
	// The CA is handed to attempts so they can verify the daemon.
	TLSCertFile string `toml:"tls_cert_file"`
	TLSKeyFile  string `toml:"tls_key_file"`
	TLSCAFile   string `toml:"tls_ca_file"`
	// InsecureHTTP allows plain HTTP on an address other hosts can reach.
	InsecureHTTP bool       `toml:"insecure_http"`
	Scheduler    Scheduler  `toml:"scheduler"`
	Node         Node       `toml:"node"`
	Bench        Bench      `toml:"bench"`
	Executors    []Executor `toml:"executors"`
	Providers    []Provider `toml:"providers"`
}

// Bench tells where the bench programs of a node are, for kairo hardware
// bench: reported to the daemon with the node (its attributes' "bench").
type Bench struct {
	// KairoBinary runs the bench driver and the CPU, storage and network
	// workloads (default: this kairo binary).
	KairoBinary string `toml:"kairo_binary"`
	// GPUBinary is kairo-bench-gpu; without it the node takes no GPU bench.
	GPUBinary string `toml:"gpu_binary"`
	// CWD is the benches' working directory (default: KairoBinary's).
	CWD string `toml:"cwd"`
	// ExecutorLabels pick the executor of the node that runs benches (one
	// that runs the binaries above natively, not a WSL one for Windows paths).
	ExecutorLabels map[string]string `toml:"executor_labels"`
}

// Attributes are node labels with the bench programs added under "bench".
func (b Bench) Attributes(labels map[string]string) map[string]any {
	out := map[string]any{}
	for k, v := range labels {
		out[k] = v
	}
	kairo := b.KairoBinary
	if kairo == "" {
		kairo, _ = os.Executable()
	}
	bench := map[string]any{}
	for key, value := range map[string]string{"kairo": kairo, "gpu": b.GPUBinary, "cwd": b.CWD} {
		if value != "" {
			bench[key] = value
		}
	}
	if len(b.ExecutorLabels) != 0 {
		bench["executor_labels"] = b.ExecutorLabels
	}
	if len(bench) != 0 {
		out["bench"] = bench
	}
	return out
}

// DatabaseURLEnv names the environment variable that may give the database
// instead of database_url (so its password need not be in the file).
const DatabaseURLEnv = "KAIRO_DATABASE_URL"

// Database returns the connection string of the daemon's database: the
// configured database_url or KAIRO_DATABASE_URL (not both), with the
// password of database_password_file when one is set.
func (c Config) Database() (string, error) {
	dsn := c.DatabaseURL
	if env := os.Getenv(DatabaseURLEnv); env != "" {
		if dsn != "" {
			return "", fmt.Errorf("the database is given twice: set database_url or %s, not both", DatabaseURLEnv)
		}
		dsn = env
	}
	if dsn == "" {
		return "", fmt.Errorf("no database: set database_url (or %s) to the PostgreSQL database", DatabaseURLEnv)
	}
	if c.DatabasePasswordFile == "" {
		return dsn, nil
	}
	body, err := os.ReadFile(c.DatabasePasswordFile)
	if err != nil {
		return "", fmt.Errorf("database_password_file: %w", err)
	}
	password, _, _ := strings.Cut(string(body), "\n")
	return WithPassword(dsn, strings.TrimRight(password, "\r"))
}

// WithPassword returns the connection string dsn with password, which it
// must not have already.
func WithPassword(dsn, password string) (string, error) {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", fmt.Errorf("database url: %w", err)
		}
		if u.User == nil {
			return "", errors.New("the database url names no user to give database_password_file's password to")
		}
		if _, set := u.User.Password(); set {
			return "", errors.New("the database url has a password and database_password_file gives another")
		}
		u.User = url.UserPassword(u.User.Username(), password)
		return u.String(), nil
	}
	for _, field := range strings.Fields(dsn) {
		if strings.HasPrefix(field, "password=") {
			return "", errors.New("the database connection string has a password and database_password_file gives another")
		}
	}
	quoted := strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(password)
	return dsn + " password='" + quoted + "'", nil
}

// RedactDatabase returns a connection string for logs and messages: its
// password replaced by xxxxx.
func RedactDatabase(dsn string) string {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		if u, err := url.Parse(dsn); err == nil {
			return u.Redacted()
		}
		return "(unparsable database url)"
	}
	fields := strings.Fields(dsn)
	for i, field := range fields {
		if strings.HasPrefix(field, "password=") {
			fields[i] = "password=xxxxx"
			// A quoted password may hold spaces: drop the rest of it.
			if strings.HasPrefix(field, "password='") && (len(field) == len("password='") || !strings.HasSuffix(field, "'")) {
				fields = fields[:i+1]
				break
			}
		}
	}
	return strings.Join(fields, " ")
}

// TLS reports whether the daemon serves HTTPS.
func (c Config) TLS() bool { return c.TLSCertFile != "" || c.TLSKeyFile != "" || c.TLSCAFile != "" }

// Scheduler holds the admission settings of the installation.
type Scheduler struct {
	// DiskMinFreeBytes is the free space every registered filesystem keeps
	// (capped at 5% of its size): a node with a filesystem under it admits
	// nothing. Unset means 5 GiB; 0 disables it.
	DiskMinFreeBytes *int64 `toml:"disk_min_free_bytes"`
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
	if c.LogMinFreeMiB < 0 {
		return c, errors.New("log_min_free_mib must not be negative")
	}
	// [node] names the control node itself. Executors and providers are
	// optional: a control-only daemon runs no jobs, and every job host runs
	// `kairo agent` instead.
	if c.Node.ID == "" || c.Node.Name == "" {
		return c, errors.New("node.id and node.name are required")
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
	if c.LogMinFreeMiB == 0 {
		c.LogMinFreeMiB = DefaultLogMinFreeMiB
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

// DefaultLogMinFreeMiB is log_min_free_mib when it is not set.
const DefaultLogMinFreeMiB = 1024

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
	LogMinFreeMiB              int        `toml:"log_min_free_mib"`
	ObserveOnly                bool       `toml:"observe_only"`
	ObservationIntervalSeconds int        `toml:"observation_interval_seconds"`
	Node                       Node       `toml:"node"`
	Bench                      Bench      `toml:"bench"`
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
		if err = checkNodeURL(u, a.InsecureHTTP); err != nil {
			return a, err
		}
	}
	if a.LogDirectory == "" {
		a.LogDirectory = "local/attempts-agent"
	}
	switch {
	case a.LogMinFreeMiB < 0:
		return a, errors.New("log_min_free_mib must not be negative")
	case a.LogMinFreeMiB == 0:
		a.LogMinFreeMiB = DefaultLogMinFreeMiB
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

// checkNodeURL refuses a URL a node token would travel over in plain HTTP,
// unless insecure is set.
func checkNodeURL(u string, insecure bool) error {
	switch scheme(u) {
	case "https":
	case "http":
		if !insecure {
			return fmt.Errorf("%q is plain HTTP: the node token would travel unencrypted; use https or set insecure_http = true", u)
		}
	default:
		return fmt.Errorf("%q is not an http(s) URL", u)
	}
	return nil
}

// Controller configures `kairo controller`: the process on a physical host
// that changes the settings of the devices its node controls (the host's
// `controller` in the machine declaration). It authenticates like an agent,
// with its node's token.
type Controller struct {
	ServerURL    string `toml:"server_url"`
	Token        string `toml:"token"`
	CAFile       string `toml:"ca_file"`
	InsecureHTTP bool   `toml:"insecure_http"`
	// Node is the controller's node; its name defaults to its id.
	Node Node `toml:"node"`
	// PollIntervalSeconds is how often the desired settings are fetched
	// (default 10). ResetAfterSeconds is how long the daemon may be
	// unreachable before every device is returned to its defaults (default
	// 120).
	PollIntervalSeconds int `toml:"poll_interval_seconds"`
	ResetAfterSeconds   int `toml:"reset_after_seconds"`
	// StateFile keeps what the controller must know across a crash: the
	// devices it controls and, on Windows, the processor settings it found
	// before it changed them. Default: next to the configuration file,
	// <name>.state.json.
	StateFile string `toml:"state_file"`
}

func LoadController(path string) (Controller, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return Controller{}, err
	}
	c := Controller{}
	metadata, err := toml.Decode(string(body), &c)
	if err != nil {
		return c, err
	}
	if undecoded := metadata.Undecoded(); len(undecoded) > 0 {
		return c, errors.New("unknown controller config field: " + undecoded[0].String())
	}
	if c.ServerURL == "" || c.Token == "" {
		return c, errors.New("server_url and token are required")
	}
	if err = checkNodeURL(c.ServerURL, c.InsecureHTTP); err != nil {
		return c, err
	}
	if c.Node.ID == "" {
		return c, errors.New("node.id is required")
	}
	if c.Node.Name == "" {
		c.Node.Name = c.Node.ID
	}
	if c.Node.OS == "" {
		c.Node.OS = runtime.GOOS
	}
	if c.Node.Architecture == "" {
		c.Node.Architecture = runtime.GOARCH
	}
	switch {
	case c.PollIntervalSeconds < 0 || c.ResetAfterSeconds < 0:
		return c, errors.New("poll_interval_seconds and reset_after_seconds must not be negative")
	case c.PollIntervalSeconds == 0:
		c.PollIntervalSeconds = 10
	}
	if c.ResetAfterSeconds == 0 {
		c.ResetAfterSeconds = 120
	}
	if c.ResetAfterSeconds < 2*c.PollIntervalSeconds {
		return c, fmt.Errorf("reset_after_seconds %d must be at least twice poll_interval_seconds %d", c.ResetAfterSeconds, c.PollIntervalSeconds)
	}
	if c.StateFile == "" {
		c.StateFile = strings.TrimSuffix(path, filepath.Ext(path)) + ".state.json"
	}
	return c, nil
}
