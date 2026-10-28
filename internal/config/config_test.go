package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const daemonBase = `
[node]
id = "host"
name = "host"
[[executors]]
id = "local"
kind = "windows"
`

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kairo.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDaemonTransportRules(t *testing.T) {
	tlsFiles := "tls_cert_file = \"server.pem\"\ntls_key_file = \"server-key.pem\"\ntls_ca_file = \"ca.pem\"\n"
	for _, c := range []struct {
		name, head, wantErr string
	}{
		{"loopback http", `listen = "127.0.0.1:7474"`, ""},
		{"localhost http", `listen = "localhost:7474"`, ""},
		{"lan http refused", `listen = "0.0.0.0:7474"`, "reachable from other hosts"},
		{"empty host refused", `listen = ":7474"`, "reachable from other hosts"},
		{"lan http allowed explicitly", "listen = \"0.0.0.0:7474\"\ninsecure_http = true", ""},
		{"https advertise without tls", "advertise_url = \"https://127.0.0.1:7474\"", "must be http"},
		{"partial tls", "listen = \"0.0.0.0:7474\"\ntls_cert_file = \"server.pem\"", "go together"},
		{"tls with http advertise", "listen = \"0.0.0.0:7474\"\nadvertise_url = \"http://192.168.1.12:7474\"\n" + tlsFiles, "must be https"},
		{"tls and insecure", "listen = \"0.0.0.0:7474\"\ninsecure_http = true\nadvertise_url = \"https://192.168.1.12:7474\"\n" + tlsFiles, "contradicts"},
		{"tls", "listen = \"0.0.0.0:7474\"\nadvertise_url = \"https://192.168.1.12:7474\"\n" + tlsFiles, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, c.head+"\n"+daemonBase))
			if c.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)) {
				t.Fatalf("error %v, want %q", err, c.wantErr)
			}
		})
	}
}

func TestTLSDefaultsAdvertiseToHTTPS(t *testing.T) {
	c, err := Load(writeConfig(t, "tls_cert_file = \"s.pem\"\ntls_key_file = \"k.pem\"\ntls_ca_file = \"ca.pem\"\n"+daemonBase))
	if err != nil {
		t.Fatal(err)
	}
	if c.AdvertiseURL != "https://127.0.0.1:7474" || !c.TLS() {
		t.Fatalf("advertise %q tls %v", c.AdvertiseURL, c.TLS())
	}
}

func TestAgentRefusesPlainHTTPUnlessAsked(t *testing.T) {
	base := "token = \"kairo_node_x\"\n[node]\nid = \"pve0\"\nname = \"pve0\"\n[[executors]]\nid = \"pve0-linux\"\nkind = \"linux\"\n"
	for _, c := range []struct {
		name, head, wantErr string
	}{
		{"https", `server_url = "https://192.168.1.12:7474"`, ""},
		{"http refused", `server_url = "http://192.168.1.12:7474"`, "plain HTTP"},
		{"http attempt url refused", "server_url = \"https://192.168.1.12:7474\"\nattempt_api_url = \"http://192.168.1.12:7474\"", "plain HTTP"},
		{"http allowed explicitly", "server_url = \"http://127.0.0.1:7474\"\ninsecure_http = true", ""},
		{"not a url", `server_url = "192.168.1.12:7474"`, "not an http(s) URL"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := LoadAgent(writeConfig(t, c.head+"\n"+base))
			if c.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)) {
				t.Fatalf("error %v, want %q", err, c.wantErr)
			}
		})
	}
}

func TestExampleConfigLoads(t *testing.T) {
	if _, err := Load(filepath.Join("..", "..", "examples", "kairo.toml")); err != nil {
		t.Fatal(err)
	}
}

func TestControlOnlyDaemonLoads(t *testing.T) {
	c, err := Load(writeConfig(t, "[node]\nid = \"control\"\nname = \"control\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Executors) != 0 || len(c.Providers) != 0 {
		t.Fatalf("executors %v providers %v", c.Executors, c.Providers)
	}
	if _, err := Load(writeConfig(t, "listen = \"127.0.0.1:7474\"\n")); err == nil || !strings.Contains(err.Error(), "node.id") {
		t.Fatalf("missing [node] accepted: %v", err)
	}
}

func TestControllerConfig(t *testing.T) {
	base := "server_url = \"https://192.168.1.12:7474\"\ntoken = \"kairo_node_x\"\n[node]\nid = \"pve0-host\"\n"
	path := writeConfig(t, base)
	c, err := LoadController(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Node.Name != "pve0-host" || c.PollIntervalSeconds != 10 || c.ResetAfterSeconds != 120 || c.StateFile != strings.TrimSuffix(path, ".toml")+".state.json" {
		t.Fatalf("defaults: %+v", c)
	}
	if c, err = LoadController("../../examples/kairo-controller.toml"); err != nil || c.Node.ID != "pve0-host" {
		t.Fatalf("example: %+v %v", c, err)
	}
	for _, bad := range []struct{ body, want string }{
		{strings.Replace(base, "https", "http", 1), "plain HTTP"},
		{"server_url = \"https://h:7474\"\ntoken = \"t\"\n", "node.id is required"},
		{"reset_after_seconds = 15\n" + base, "at least twice"},
		{"executors = []\n" + base, "unknown controller config field"},
	} {
		if _, err = LoadController(writeConfig(t, bad.body)); err == nil || !strings.Contains(err.Error(), bad.want) {
			t.Fatalf("%q: %v, want %q", bad.body, err, bad.want)
		}
	}
}

func TestAgentStillRequiresAnExecutor(t *testing.T) {
	_, err := LoadAgent(writeConfig(t, "server_url = \"https://192.168.1.12:7474\"\ntoken = \"kairo_node_x\"\n[node]\nid = \"pve0\"\nname = \"pve0\"\n"))
	if err == nil || !strings.Contains(err.Error(), "at least one executor") {
		t.Fatalf("agent without executors: %v", err)
	}
}

func TestLogMinFreeDefaultsAndRefusesNegative(t *testing.T) {
	agentBase := "server_url = \"https://192.168.1.12:7474\"\ntoken = \"kairo_node_x\"\n[node]\nid = \"pve0\"\nname = \"pve0\"\n[[executors]]\nid = \"pve0-linux\"\nkind = \"linux\"\n"
	c, err := Load(writeConfig(t, daemonBase))
	if err != nil || c.LogMinFreeMiB != DefaultLogMinFreeMiB {
		t.Fatalf("daemon default: %d %v", c.LogMinFreeMiB, err)
	}
	a, err := LoadAgent(writeConfig(t, "log_min_free_mib = 256\n"+agentBase))
	if err != nil || a.LogMinFreeMiB != 256 {
		t.Fatalf("agent setting: %d %v", a.LogMinFreeMiB, err)
	}
	if a, err = LoadAgent(writeConfig(t, agentBase)); err != nil || a.LogMinFreeMiB != DefaultLogMinFreeMiB {
		t.Fatalf("agent default: %d %v", a.LogMinFreeMiB, err)
	}
	if _, err = Load(writeConfig(t, "log_min_free_mib = -1\n"+daemonBase)); err == nil || !strings.Contains(err.Error(), "log_min_free_mib") {
		t.Fatalf("negative daemon setting: %v", err)
	}
	if _, err = LoadAgent(writeConfig(t, "log_min_free_mib = -1\n"+agentBase)); err == nil || !strings.Contains(err.Error(), "log_min_free_mib") {
		t.Fatalf("negative agent setting: %v", err)
	}
}

func TestDatabaseFromTheFileTheEnvironmentAndAPasswordFile(t *testing.T) {
	t.Setenv(DatabaseURLEnv, "")
	password := filepath.Join(t.TempDir(), "db-password")
	if err := os.WriteFile(password, []byte("s3cr'et\r\nignored\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name, url, env, passwordFile, want, wantErr string
	}{
		{"none", "", "", "", "", "no database"},
		{"file", "postgres://kairo@db:5432/kairo", "", "", "postgres://kairo@db:5432/kairo", ""},
		{"environment", "", "postgres://kairo:pw@db/kairo", "", "postgres://kairo:pw@db/kairo", ""},
		{"both", "postgres://a@db/kairo", "postgres://b@db/kairo", "", "", "not both"},
		{"url with password file", "postgres://kairo@db/kairo?sslmode=require", "", password, "postgres://kairo:s3cr%27et@db/kairo?sslmode=require", ""},
		{"key=value with password file", "host=db user=kairo dbname=kairo", "", password, `host=db user=kairo dbname=kairo password='s3cr\'et'`, ""},
		{"two passwords", "postgres://kairo:pw@db/kairo", "", password, "", "has a password"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(DatabaseURLEnv, c.env)
			body := daemonBase
			if c.url != "" {
				body = "database_url = '" + c.url + "'\n" + body
			}
			if c.passwordFile != "" {
				body = "database_password_file = '" + c.passwordFile + "'\n" + body
			}
			cfg, err := Load(writeConfig(t, body))
			if err != nil {
				t.Fatal(err)
			}
			got, err := cfg.Database()
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("got %q, %v; want error %q", got, err, c.wantErr)
				}
				return
			}
			if err != nil || got != c.want {
				t.Fatalf("got %q, %v; want %q", got, err, c.want)
			}
		})
	}
}
