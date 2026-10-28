package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"text/tabwriter"

	"kairo/internal/config"
	"kairo/internal/sqliteimport"
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

// dbImportSQLite: `kairo db import-sqlite --from KAIRO.db (--to URL |
// --config DAEMON.toml)` copies a SQLite Kairo database into an empty
// PostgreSQL one and prints the verification of every table.
func dbImportSQLite(args []string) error {
	fs := newFlags("db import-sqlite")
	from := fs.String("from", "", "the SQLite database file (read only)")
	to := fs.String("to", "", "the empty PostgreSQL database")
	configPath := fs.String("config", "", "daemon configuration (its database is the target)")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 0 || *from == "" || (*to == "") == (*configPath == "") {
		return usageError("usage: kairo db import-sqlite --from KAIRO.db (--to URL | --config DAEMON.toml)")
	}
	dsn := *to
	if *configPath != "" {
		daemonConfig, err := config.Load(*configPath)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}
		if dsn, err = daemonConfig.Database(); err != nil {
			return err
		}
	}
	fmt.Fprintf(os.Stderr, "kairo: importing %s into %s\n", *from, config.RedactDatabase(dsn))
	results, err := sqliteimport.Import(context.Background(), *from, dsn)
	if len(results) > 0 {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "TABLE\tSQLITE ROWS\tPOSTGRES ROWS\tSQLITE SHA256\tPOSTGRES SHA256\tRESULT")
		var rows int64
		for _, r := range results {
			verdict := "ok"
			if !r.OK() {
				verdict = "MISMATCH"
			}
			rows += r.TargetRows
			fmt.Fprintf(w, "%s\t%d\t%d\t%.16s\t%.16s\t%s\n", r.Table, r.SourceRows, r.TargetRows, r.SourceSum, r.TargetSum, verdict)
		}
		w.Flush()
		if err == nil {
			fmt.Fprintf(os.Stderr, "kairo: imported %d tables, %d rows; every table verified\n", len(results), rows)
		}
	}
	if errors.Is(err, sqliteimport.ErrMismatch) {
		return err
	}
	if err != nil {
		return fmt.Errorf("import: %w", err)
	}
	return nil
}
