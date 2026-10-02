package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"kairo/internal/config"
	"kairo/internal/secfile"
	"kairo/internal/store"
)

const tokenUsage = "usage: kairo token create (--config DAEMON.toml | --db PATH) --name NAME --role admin|read|node [--node NODE_ID] [--save | --out FILE] [--force]\n" +
	"       kairo token list (--config DAEMON.toml | --db PATH)\n" +
	"       kairo token revoke (--config DAEMON.toml | --db PATH) ID_OR_NAME"

// tokenCommand manages API tokens directly in the daemon's database (it is
// safe while the daemon runs), so the first admin token needs no token.
func tokenCommand(args []string) error {
	if len(args) == 0 {
		return errors.New(tokenUsage)
	}
	action := args[0]
	if action != "create" && action != "list" && action != "revoke" {
		return fmt.Errorf("unknown token command %q\n%s", action, tokenUsage)
	}
	fs := flag.NewFlagSet("token "+action, flag.ContinueOnError)
	configPath := fs.String("config", "", "daemon TOML configuration (its database_path is used)")
	dbPath := fs.String("db", "", "daemon database path")
	var name, role, node, out *string
	var save, force *bool
	if action == "create" {
		name = fs.String("name", "", "token name (unique among active tokens)")
		role = fs.String("role", "", "admin, read or node")
		node = fs.String("node", "", "with --role node: the node the token is bound to")
		save = fs.Bool("save", false, "write the token to <user config dir>/kairo/token instead of printing it")
		out = fs.String("out", "", "write the token to FILE instead of printing it")
		force = fs.Bool("force", false, "with --save or --out: replace an existing file")
	}
	positional, err := parseInterleaved(fs, args[1:])
	if err != nil {
		return err
	}
	switch action {
	case "revoke":
		if len(positional) != 1 {
			return errors.New("token revoke requires ID_OR_NAME")
		}
	default:
		if len(positional) != 0 {
			return fmt.Errorf("token %s takes no positional arguments", action)
		}
	}
	var target string
	if action == "create" {
		if *save && *out != "" {
			return errors.New("--save and --out are exclusive")
		}
		if *force && !*save && *out == "" {
			return errors.New("--force applies only with --save or --out")
		}
		if *role == store.RoleNode && *node == "" {
			return errors.New("a node token needs --node NODE_ID")
		}
		target = *out
		if *save {
			dir, err := kairoConfigDir()
			if err != nil {
				return err
			}
			target = filepath.Join(dir, "token")
		}
		if target != "" && !*force {
			if _, err := os.Stat(target); err == nil {
				return fmt.Errorf("%s exists; pass --force to replace it", target)
			}
		}
	}
	st, err := openTokenStore(*configPath, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	ctx := context.Background()
	switch action {
	case "create":
		return tokenCreate(ctx, st, *name, *role, *node, target, *force)
	case "list":
		return tokenList(ctx, st)
	default:
		if err := st.RevokeAPIToken(ctx, positional[0]); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return fmt.Errorf("no active token with id or name %q", positional[0])
			}
			return err
		}
		fmt.Println("revoked", positional[0])
		return nil
	}
}

// parseInterleaved parses flags given before, between or after positional
// arguments (the flag package stops at the first positional one).
func parseInterleaved(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		rest := fs.Args()
		if len(rest) == 0 {
			return positional, nil
		}
		// After "--" everything is positional.
		if len(args) > len(rest) && args[len(args)-len(rest)-1] == "--" {
			return append(positional, rest...), nil
		}
		positional = append(positional, rest[0])
		args = rest[1:]
	}
}

func openTokenStore(configPath, dbPath string) (*store.Store, error) {
	if (configPath == "") == (dbPath == "") {
		return nil, errors.New("token commands need exactly one of --config or --db")
	}
	if configPath != "" {
		daemonConfig, err := config.Load(configPath)
		if err != nil {
			return nil, fmt.Errorf("load config: %w", err)
		}
		dbPath = daemonConfig.DatabasePath
	}
	// Never create a database here: a token in a database the daemon does not
	// use would never authenticate.
	if _, err := os.Stat(dbPath); err != nil {
		return nil, fmt.Errorf("no Kairo database at %s (run `kairo serve` once to create it; a relative database_path is relative to the current directory): %w", dbPath, err)
	}
	return store.Open(dbPath)
}

func tokenCreate(ctx context.Context, st *store.Store, name, role, node, target string, force bool) error {
	token, plaintext, err := st.CreateAPIToken(ctx, name, role, node)
	if err != nil {
		return err
	}
	note := fmt.Sprintf("kairo: %s token %q (%s); the token is shown only now and cannot be recovered", token.Role, token.Name, token.ID)
	if target == "" {
		fmt.Fprintln(os.Stderr, note)
		fmt.Println(plaintext)
		return nil
	}
	if err := writeSecretFile(target, plaintext+"\n", force); err != nil {
		// The plaintext is lost: do not leave a token nobody holds.
		if revokeErr := st.RevokeAPIToken(ctx, token.ID); revokeErr != nil {
			return fmt.Errorf("%w (and revoking token %s failed: %v)", err, token.ID, revokeErr)
		}
		return fmt.Errorf("%w (token %s revoked)", err, token.ID)
	}
	fmt.Fprintln(os.Stderr, strings.Replace(note, "shown only now", "written only to this file", 1))
	fmt.Println(target)
	return nil
}

// writeSecretFile writes a secret readable by its owner only (0600 in a 0700
// directory it creates), refusing to replace a file unless force is set.
func writeSecretFile(path, content string, force bool) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if force {
		flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0o600)
	if errors.Is(err, os.ErrExist) {
		return fmt.Errorf("%s exists; pass --force to replace it", path)
	}
	if err != nil {
		return err
	}
	if err := f.Chmod(0o600); err != nil && !errors.Is(err, errors.ErrUnsupported) {
		f.Close()
		return err
	}
	if _, err := f.WriteString(content); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// 0600 means nothing on Windows: replace the inherited DACL.
	return secfile.Restrict(path)
}

func tokenList(ctx context.Context, st *store.Store) error {
	tokens, err := st.ListAPITokens(ctx)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tROLE\tNODE\tCREATED\tLAST USED\tREVOKED")
	orDash := func(value *string) string {
		if value == nil || *value == "" {
			return "-"
		}
		return *value
	}
	for _, t := range tokens {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", t.ID, t.Name, t.Role, orDash(t.NodeID), t.CreatedAt, orDash(t.LastUsedAt), orDash(t.RevokedAt))
	}
	return w.Flush()
}
