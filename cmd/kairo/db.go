package main

import (
	"context"
	"fmt"
	"os"
)

func dbCompact(args []string) error {
	fs := newFlags("db compact")
	configPath := fs.String("config", "", "daemon configuration (its database)")
	databaseURL := fs.String("database-url", "", "the daemon's PostgreSQL database")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 0 {
		return usageError("usage: kairo db compact (--config DAEMON.toml | --database-url URL)")
	}
	st, err := openTokenStore(*configPath, *databaseURL)
	if err != nil {
		return err
	}
	defer st.Close()
	removed, err := st.Compact(context.Background())
	if err != nil {
		return fmt.Errorf("compact: %w", err)
	}
	fmt.Fprintf(os.Stderr, "kairo: pruned %d resource observations and device samples and vacuumed their tables\n", removed)
	return nil
}
