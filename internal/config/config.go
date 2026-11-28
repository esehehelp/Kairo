package config

import (
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

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
	LogDirectory         string `toml:"log_directory"`
	// SourceDirectory keeps the code snapshots kairo project apply sends
	// (api/sources.go; default: sources beside log_directory).
	SourceDirectory string `toml:"source_directory"`
	// Fabric is the network every node is on (issue #47; store/fabric.go):
	// without a prefix there is none.
	Fabric DaemonFabric `toml:"fabric"`
	// ControlSnapshotMinutes: how often the control plane is snapshotted
	// into the durable store (README: Kairo keeps itself); 0: every 60,
	// negative: never.
	ControlSnapshotMinutes int `toml:"control_snapshot_minutes"`
	// PGDump: the pg_dump that dumps the database for a control snapshot
	// (default: pg_dump on PATH).
	PGDump string `toml:"pg_dump"`
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
	Data         Data       `toml:"data"`
	Node         Node       `toml:"node"`
	Bench        Bench      `toml:"bench"`
	Executors    []Executor `toml:"executors"`
	Providers    []Provider `toml:"providers"`
}

// Bench is a [bench] table, which no configuration has any more: a bench
// runs in a container, with the programs Kairo carries (README "Hardware
// benches"). One still there is refused with that said.
type Bench map[string]any

// refuse is the error of a configuration that still has [bench].
func (b Bench) refuse() error {
	if len(b) == 0 {
		return nil
	}
	return errors.New("[bench] is gone: hardware benches run in containers with the programs Kairo carries; remove the [bench] table")
}

// NodeAttributes are what a node reports of itself: its labels.
func NodeAttributes(labels map[string]string) map[string]any {
	out := make(map[string]any, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	return out
}

// DatabaseURLEnv names the environment variable that may give the database
// instead of database_url (so its password need not be in the file).
const DatabaseURLEnv = "KAIRO_DATABASE_URL"

// ControlSnapshotEvery is how often the control plane is snapshotted (0:
// never).
func (c Config) ControlSnapshotEvery() time.Duration {
	switch {
	case c.ControlSnapshotMinutes < 0:
		return 0
	case c.ControlSnapshotMinutes == 0:
		return time.Hour
	}
	return time.Duration(c.ControlSnapshotMinutes) * time.Minute
}

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

// HasPassword reports whether the connection string dsn carries a password
// (one it cannot read is taken to).
func HasPassword(dsn string) bool {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return true
		}
		if u.User != nil {
			if _, set := u.User.Password(); set {
				return true
			}
		}
		return u.Query().Has("password")
	}
	for _, field := range strings.Fields(dsn) {
		if strings.HasPrefix(field, "password=") {
			return true
		}
	}
	return false
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

// Data holds the data settings: how often each verified copy on a durable
// store is read back ("168h" by default, a Go duration) and how many bytes a
// durable store reads back a day at most (200 GiB by default).
type Data struct {
	ScrubInterval    string `toml:"scrub_interval"`
	ScrubBytesPerDay int64  `toml:"scrub_bytes_per_day"`
	// ConfineFrom (RFC 3339 with an offset): every attempt launched from
	// then on is confined, whether its execution asks or not.
	ConfineFrom string `toml:"confine_from"`
	// CheckpointInterval (a Go duration; default "2h", "0" never): a running
	// checkpointable attempt that has not published a checkpoint for this
	// long is asked for one, which is then kept like any data (README:
	// Checkpoints of running work).
	CheckpointInterval string `toml:"checkpoint_interval"`
	// RunSnapshotInterval (a Go duration; default "30m", "0" never): how
	// often a running run's output directory is snapshotted, and after each
	// checkpoint it publishes (README: Data stores, run snapshots).
	RunSnapshotInterval string `toml:"run_snapshot_interval"`
}

// RunSnapshotEvery is RunSnapshotInterval parsed (0: never).
func (d Data) RunSnapshotEvery() (time.Duration, error) {
	if d.RunSnapshotInterval == "" {
		return 30 * time.Minute, nil
	}
	every, err := time.ParseDuration(d.RunSnapshotInterval)
	if err != nil || every < 0 {
		return 0, fmt.Errorf("data.run_snapshot_interval %q is not a duration such as \"30m\" (\"0\": never)", d.RunSnapshotInterval)
	}
	return every, nil
}

// CheckpointEvery is CheckpointInterval parsed (0: never).
func (d Data) CheckpointEvery() (time.Duration, error) {
	if d.CheckpointInterval == "" {
		return 2 * time.Hour, nil
	}
	every, err := time.ParseDuration(d.CheckpointInterval)
	if err != nil || every < 0 {
		return 0, fmt.Errorf("data.checkpoint_interval %q is not a duration such as \"2h\" (\"0\": never)", d.CheckpointInterval)
	}
	return every, nil
}

// ConfineTime is ConfineFrom parsed (zero: not set).
func (d Data) ConfineTime() (time.Time, error) {
	if d.ConfineFrom == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, d.ConfineFrom)
	if err != nil {
		return time.Time{}, fmt.Errorf("data.confine_from %q is not a time with an offset such as \"2026-11-14T00:00:00+09:00\"", d.ConfineFrom)
	}
	return t, nil
}

// ScrubEvery is ScrubInterval parsed (0: not set).
func (d Data) ScrubEvery() (time.Duration, error) {
	if d.ScrubInterval == "" {
		return 0, nil
	}
	every, err := time.ParseDuration(d.ScrubInterval)
	if err != nil || every <= 0 {
		return 0, fmt.Errorf("data.scrub_interval %q is not a positive duration such as \"168h\"", d.ScrubInterval)
	}
	return every, nil
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

// KindIncus is an executor that runs every attempt in an Incus container
// of the attempt's image (executor/container.go), on Linux.
const KindIncus = "incus"

// KindWSLC is an executor that runs every attempt in a WSL container (wslc)
// of the attempt's image, made for it and removed after it
// (executor/container_wslc.go), by an agent on Windows.
const KindWSLC = "wslc"

// containerKinds are the kinds whose executors run containers, and the
// system their agent runs on: they set the "runtime" attribute.
var containerKinds = map[string]string{KindIncus: "linux", KindWSLC: "windows"}

// Attributes are what the executor registers to be matched by executor
// selectors: its labels, and "runtime" naming its container runtime when it
// runs containers (store.ContainerRuntimeAttribute), which only its kind
// sets.
func (e Executor) Attributes() (map[string]string, error) {
	out := make(map[string]string, len(e.Labels)+1)
	maps.Copy(out, e.Labels)
	if _, ok := e.Labels["runtime"]; ok {
		return nil, fmt.Errorf("executor %s: label runtime is set by its kind, not declared", e.ID)
	}
	if containerKinds[e.Kind] != "" {
		out["runtime"] = e.Kind
	}
	return out, nil
}

// checkExecutors refuses executors whose attributes cannot be made, or that
// run no containers (every executor runs its attempts in containers of
// their images), or whose runtime is another system's.
func checkExecutors(executors []Executor) error {
	for _, e := range executors {
		if _, err := e.Attributes(); err != nil {
			return err
		}
		system := containerKinds[e.Kind]
		if system == "" {
			return fmt.Errorf("executor %s: kind %q runs no containers; every executor runs its attempts in containers (incus on Linux, wslc on Windows)", e.ID, e.Kind)
		}
		if runtime.GOOS != system {
			return fmt.Errorf("executor %s: kind %s runs on %s only", e.ID, e.Kind, system)
		}
	}
	return nil
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
	if _, err := c.Data.ScrubEvery(); err != nil {
		return c, err
	}
	if _, err := c.Data.CheckpointEvery(); err != nil {
		return c, err
	}
	if _, err := c.Data.RunSnapshotEvery(); err != nil {
		return c, err
	}
	if c.Data.ScrubBytesPerDay < 0 {
		return c, errors.New("data.scrub_bytes_per_day must not be negative")
	}
	// [node] names the control node itself. The daemon runs no jobs: every
	// executor is a node agent's (`kairo agent`), whose containers run them.
	if c.Node.ID == "" || c.Node.Name == "" {
		return c, errors.New("node.id and node.name are required")
	}
	if len(c.Executors) > 0 || len(c.Providers) > 0 {
		return c, errors.New("the daemon runs no executors or providers: remove [[executors]] and [[providers]]; work runs on node agents (kairo agent)")
	}
	if err := c.Bench.refuse(); err != nil {
		return c, err
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

// absPath is path made absolute against the working directory now: a
// directory of attempt records and exit files is passed to processes that
// run elsewhere (an attempt's shim runs in the attempt's cwd).
func absPath(path string) string {
	if abs, err := filepath.Abs(path); err == nil {
		return abs
	}
	return path
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
	if c.SourceDirectory == "" {
		c.SourceDirectory = filepath.Join(filepath.Dir(filepath.Clean(c.LogDirectory)), "sources")
	}
	c.LogDirectory, c.SourceDirectory = absPath(c.LogDirectory), absPath(c.SourceDirectory)
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
	Packs                      Packs      `toml:"packs"`
	// OwnerUse: how this node's providers tell its owner's use of a
	// resource (README: Owner use).
	OwnerUse OwnerUse `toml:"owner_use"`
	// ContainerPrefix begins the names of the containers this agent makes
	// for attempts (default kairo-): another installation's agent on the
	// same host takes another (executor.ContainerPrefix).
	ContainerPrefix string `toml:"container_prefix"`
	// Fabric puts this node on the fabric (issue #47).
	Fabric AgentFabric `toml:"fabric"`
}

// OwnerUse is an agent's [owner_use]: on unless disabled; a threshold left
// zero takes its default (provider.OwnerUse). The *_present ones apply while
// the owner is at the keyboard (input within 10 minutes, as input_file has
// it: on Windows %LOCALAPPDATA%/kairo/input.json by default, written by
// kairo input-report in the owner's session). KairoFirst: Kairo's work
// comes before the owner on this node (provider.OwnerUse).
type OwnerUse struct {
	Disabled            bool    `toml:"disabled"`
	KairoFirst          bool    `toml:"kairo_first"`
	GPUSMPercent        float64 `toml:"gpu_sm_percent"`
	GPUSMPercentPresent float64 `toml:"gpu_sm_percent_present"`
	CPUPercent          float64 `toml:"cpu_percent"`
	CPUPercentPresent   float64 `toml:"cpu_percent_present"`
	RAMFreeMiB          int64   `toml:"ram_free_mib"`
	RAMFreeMiBPresent   int64   `toml:"ram_free_mib_present"`
	InputFile           string  `toml:"input_file"`
}

// Packs is this agent's pack server, from which other agents fetch the packs
// made here to copy them to a durable store: Listen is where it listens
// (":7475"), Addr the host:port other agents reach it at (advertised as each
// executor's pack_addr), CertFile and KeyFile its TLS certificate for that
// host (kairo tls issue-server, from the daemon's CA). Without Listen there is
// none, and packs made here are copied only by an executor of this node.
type Packs struct {
	Listen   string `toml:"listen"`
	Addr     string `toml:"addr"`
	CertFile string `toml:"cert_file"`
	KeyFile  string `toml:"key_file"`
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
	if a.Fabric.Enabled {
		if a.Fabric.Interface == "" {
			a.Fabric.Interface = "kairo0"
		}
		if a.Fabric.ListenPort == 0 {
			a.Fabric.ListenPort = 51820
		}
		if a.Fabric.KeyFile == "" {
			a.Fabric.KeyFile = filepath.Join(filepath.Dir(path), "fabric.key")
		}
		if a.Fabric.BootstrapURL != "" {
			if err := checkNodeURL(a.Fabric.BootstrapURL, a.InsecureHTTP); err != nil {
				return a, err
			}
		}
		if n := a.Fabric.Names; n != "" && !validFabricNames(n) {
			return a, fmt.Errorf("fabric.names %q: 1 to 5 lowercase letters or digits, a letter first", n)
		}
	}
	if a.LogDirectory == "" {
		a.LogDirectory = "local/attempts-agent"
	}
	a.LogDirectory = absPath(a.LogDirectory)
	switch {
	case a.LogMinFreeMiB < 0:
		return a, errors.New("log_min_free_mib must not be negative")
	case a.LogMinFreeMiB == 0:
		a.LogMinFreeMiB = DefaultLogMinFreeMiB
	}
	if a.ObservationIntervalSeconds == 0 {
		a.ObservationIntervalSeconds = 5
	}
	if o := a.OwnerUse; o.GPUSMPercent < 0 || o.GPUSMPercentPresent < 0 || o.CPUPercent < 0 || o.CPUPercentPresent < 0 || o.RAMFreeMiB < 0 || o.RAMFreeMiBPresent < 0 ||
		o.GPUSMPercent > 100 || o.GPUSMPercentPresent > 100 || o.CPUPercent > 100 || o.CPUPercentPresent > 100 {
		return a, errors.New("owner_use: thresholds are percentages (0-100) and MiB, not negative")
	}
	if p := a.Packs; p.Listen != "" || p.Addr != "" || p.CertFile != "" || p.KeyFile != "" {
		// on the fabric, addr may be left out: the node's fabric address
		// names the endpoint (issue #47)
		// (not on Windows: the fabric is in the wslc VM, not the agent's)
		if p.Listen == "" || p.Addr == "" && (!a.Fabric.Enabled || runtime.GOOS == "windows") || p.CertFile == "" || p.KeyFile == "" {
			return a, errors.New("packs needs listen, addr, cert_file and key_file together (addr may be left out on the fabric, but for Windows)")
		}
		if p.Addr != "" {
			if _, port, err := net.SplitHostPort(p.Addr); err != nil || port == "" {
				return a, fmt.Errorf("packs.addr %q is not host:port", p.Addr)
			}
		}
		if p.Addr == "" {
			if _, port, err := net.SplitHostPort(p.Listen); err != nil || port == "" {
				return a, fmt.Errorf("packs.listen %q is not host:port (its port names the endpoint at the fabric address)", p.Listen)
			}
		}
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
	if err := checkExecutors(a.Executors); err != nil {
		return a, err
	}
	if err := a.Bench.refuse(); err != nil {
		return a, err
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

// DaemonFabric is the daemon's [fabric]: Prefix is the IPv4 network the
// nodes' fabric addresses are given from (each node's once, for good).
// With Join the daemon's own node is on the fabric too, as an agent's is
// (the other fields as AgentFabric's), so that nodes reach the daemon at
// its fabric address.
type DaemonFabric struct {
	Prefix string `toml:"prefix"`
	// ASN and VXLANPort are how the fabric's routing runs (0: a private
	// 4-byte ASN, 4789); SegmentPrefix is where segments' /24 subnets are
	// taken from ("": no segments). Issue #47, phase 2.
	ASN           uint32   `toml:"asn"`
	VXLANPort     int      `toml:"vxlan_port"`
	SegmentPrefix string   `toml:"segment_prefix"`
	Join          bool     `toml:"join"`
	RelayPorts    string   `toml:"relay_ports"`
	Interface     string   `toml:"interface"`
	ListenPort    int      `toml:"listen_port"`
	KeyFile       string   `toml:"key_file"`
	Endpoints     []string `toml:"endpoints"`
}

// AgentFabric is an agent's [fabric] (issue #47): when Enabled, the agent
// puts its node on the fabric, a WireGuard interface (Interface, default
// kairo0) listening on ListenPort (default 51820), with its key in KeyFile
// (default fabric.key beside the agent's configuration; made the first
// time), and says its peers may reach it at Endpoints ("host:port", in its
// order of preference). Its transfer endpoint is then advertised at its
// fabric address (pack_addr), unless [packs] addr says otherwise.
type AgentFabric struct {
	Enabled bool `toml:"enabled"`
	// BootstrapURL is the daemon as reached off the fabric (its LAN or
	// public address): a node that is not on the fabric yet (its first
	// start, or every peer moved) says what it is there, while server_url
	// names the daemon's fabric address.
	BootstrapURL string `toml:"bootstrap_url"`
	// RelayPorts ("FROM-TO") are the UDP ports this node offers to relay
	// other nodes' WireGuard datagrams when they reach each other on no
	// direct path (a node with a public endpoint; "": it relays none).
	RelayPorts string `toml:"relay_ports"`
	// Routing (default true) runs FRR on the node and carries segments
	// (issue #47 phase 2); Names prefixes the segments' devices (default
	// "kf": kfb10000, kfv10000; another installation on the same host needs
	// its own).
	Routing *bool  `toml:"routing"`
	Names   string `toml:"names"`
	// Metered says the node's uplink is paid by the byte (a mobile line): a
	// gang spans its links only when it says it may (issue #47 phase 5).
	Metered    bool     `toml:"metered"`
	Interface  string   `toml:"interface"`
	ListenPort int      `toml:"listen_port"`
	KeyFile    string   `toml:"key_file"`
	Endpoints  []string `toml:"endpoints"`
}

// ParseRelayPorts reads relay_ports ("FROM-TO", two or more ports from
// 1024): (0, 0) for "".
func ParseRelayPorts(s string) (int, int, error) {
	if s == "" {
		return 0, 0, nil
	}
	from, to, ok := strings.Cut(s, "-")
	a, err1 := strconv.Atoi(strings.TrimSpace(from))
	b, err2 := strconv.Atoi(strings.TrimSpace(to))
	if !ok || err1 != nil || err2 != nil || a < 1024 || b > 65535 || a+1 > b {
		return 0, 0, fmt.Errorf("relay_ports %q: FROM-TO, two or more ports from 1024", s)
	}
	return a, b, nil
}

// validFabricNames: a segment's devices are named NAMES + "b"/"v" + its VNI
// (up to 8 digits), within an interface name's 15 characters.
func validFabricNames(n string) bool {
	if len(n) < 1 || len(n) > 5 || n[0] < 'a' || n[0] > 'z' {
		return false
	}
	for _, c := range n {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// FabricRouting says whether an agent on the fabric runs FRR (default yes).
func (f AgentFabric) FabricRouting() bool { return f.Routing == nil || *f.Routing }
