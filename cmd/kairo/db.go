package main

import (
	"context"
	"fmt"
	"os"
)

func dbCompact(args []string) error {
	fs := newFlags("db compact")
	configPath := fs.String("config", "", "daemon configuration (its database_path)")
	dbPath := fs.String("db", "", "database file")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 0 {
		return usageError("usage: kairo db compact (--config DAEMON.toml | --db PATH)")
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
