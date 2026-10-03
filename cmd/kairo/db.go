package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
)

// dbCommand maintains the daemon's database directly (like kairo token).
func dbCommand(args []string) error {
	if len(args) == 0 || args[0] != "compact" {
		return errors.New("usage: kairo db compact (--config DAEMON.toml | --db PATH)")
	}
	fs := flag.NewFlagSet("db compact", flag.ContinueOnError)
	configPath := fs.String("config", "", "daemon configuration (its database_path)")
	dbPath := fs.String("db", "", "database file")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: kairo db compact (--config DAEMON.toml | --db PATH)")
	}
	st, err := openTokenStore(*configPath, *dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	removed, err := st.Compact(context.Background())
	if err != nil {
		return fmt.Errorf("compact (stop the daemon first: VACUUM needs the database to itself): %w", err)
	}
	fmt.Fprintf(os.Stderr, "kairo: pruned %d resource observations and compacted the database\n", removed)
	return nil
}
