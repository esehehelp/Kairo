// Package sqliteimport copies a Kairo database of the SQLite era (a
// kairo-v3.db file) into an empty PostgreSQL database, in one transaction,
// and verifies every table before committing: its row count and a checksum
// computed the same way from both databases. It is the only part of Kairo
// that reads SQLite.
package sqliteimport

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	"kairo/internal/store"
)

// sqliteOnly are the SQLite tables that have no PostgreSQL counterpart:
// their schema bookkeeping (replaced by schema_version) and SQLite's own.
var sqliteOnly = map[string]bool{"schema_migrations": true, "orchestration_schema_migrations": true, "sqlite_sequence": true}

// Result is the verification of one table.
type Result struct {
	Table      string
	SourceRows int64
	TargetRows int64
	SourceSum  string
	TargetSum  string
}

// OK reports whether the table was copied exactly.
func (r Result) OK() bool { return r.SourceRows == r.TargetRows && r.SourceSum == r.TargetSum }

// ErrMismatch: a table differs after the copy; nothing was committed.
var ErrMismatch = errors.New("a table differs between the SQLite and the PostgreSQL database; nothing was imported")

type column struct {
	name     string
	pgType   string // data_type of information_schema: text, bigint, boolean, double precision
	nullable bool
	identity bool
}

type table struct {
	name    string
	columns []column
	key     []string // primary key columns, or every column
	hasKey  bool
	parents []string // tables it references
	// selfRef: a column referencing this table's own key (coordination_scopes.parent_id).
	selfRef, selfKey string
}

// Import copies the SQLite database at path into the empty PostgreSQL
// database dsn. It returns the verification of every table; on ErrMismatch
// or any other error nothing is committed.
func Import(ctx context.Context, path, dsn string) ([]Result, error) {
	source, err := openSQLite(path)
	if err != nil {
		return nil, err
	}
	defer source.Close()
	// One read transaction: every table is read from one snapshot.
	src, err := source.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	defer src.Rollback()
	if err := checkSource(ctx, src); err != nil {
		return nil, err
	}

	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("database url: %w", err)
	}
	target := stdlib.OpenDB(*config)
	defer target.Close()
	conn, err := target.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := beginWriteOn(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("connect to the database: %w", err)
	}
	defer tx.Rollback()
	if err := store.CreateSchemaAt(ctx, tx, sqliteSchemaVersion); err != nil {
		return nil, fmt.Errorf("create the schema: %w", err)
	}
	tables, err := targetTables(ctx, tx)
	if err != nil {
		return nil, err
	}
	sourceTables, err := sqliteTables(ctx, src)
	if err != nil {
		return nil, err
	}
	for name := range sourceTables {
		if !sqliteOnly[name] && !hasTable(tables, name) {
			return nil, fmt.Errorf("the SQLite database has a table %s that Kairo does not know; is it from a newer Kairo?", name)
		}
	}
	// Triggers check what the code inserts one row at a time; the copy is
	// of rows that passed them already, in an order of their own.
	if _, err := tx.ExecContext(ctx, `ALTER TABLE lease_items DISABLE TRIGGER exclusive_resource_lease_guard`); err != nil {
		return nil, err
	}
	var results []Result
	for _, t := range tables {
		sourceColumns, ok := sourceTables[t.name]
		if !ok {
			return nil, fmt.Errorf("the SQLite database has no table %s: open it once with the last SQLite Kairo so its schema is current", t.name)
		}
		for _, c := range t.columns {
			if !sourceColumns[c.name] {
				return nil, fmt.Errorf("the SQLite table %s has no column %s: open it once with the last SQLite Kairo so its schema is current", t.name, c.name)
			}
		}
		for name := range sourceColumns {
			if !t.hasColumn(name) {
				return nil, fmt.Errorf("the SQLite table %s has a column %s that Kairo does not know", t.name, name)
			}
		}
		n, err := copyTable(ctx, src, conn, t)
		if err != nil {
			return nil, fmt.Errorf("copy %s: %w", t.name, err)
		}
		results = append(results, Result{Table: t.name, SourceRows: n})
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE lease_items ENABLE TRIGGER exclusive_resource_lease_guard`); err != nil {
		return nil, err
	}
	if err := resetIdentities(ctx, src, tx, tables); err != nil {
		return nil, err
	}
	mismatch := false
	for i, t := range tables {
		r := &results[i]
		var sourceRows int64
		if r.SourceSum, sourceRows, err = checksum(ctx, src, t, false); err != nil {
			return nil, fmt.Errorf("checksum %s in SQLite: %w", t.name, err)
		}
		if r.TargetSum, r.TargetRows, err = checksum(ctx, tx, t, true); err != nil {
			return nil, fmt.Errorf("checksum %s in PostgreSQL: %w", t.name, err)
		}
		if sourceRows != r.SourceRows {
			return nil, fmt.Errorf("%s changed while it was copied", t.name)
		}
		if !r.OK() {
			mismatch = true
		}
	}
	if mismatch {
		return results, ErrMismatch
	}
	// What was copied is verified: the later migrations apply to it as they
	// would to a database the daemon had kept since.
	if err := store.MigrateTx(ctx, tx); err != nil {
		return nil, fmt.Errorf("migrate the schema: %w", err)
	}
	return results, tx.Commit()
}

// sqliteSchemaVersion is the PostgreSQL schema migration the last SQLite
// Kairo's schema corresponds to: the copy is made into it.
const sqliteSchemaVersion = 1

// beginWriteOn begins the import's transaction on conn, under the decision
// lock as every write (a daemon pointed at the database waits for it).
func beginWriteOn(ctx context.Context, conn *sql.Conn) (*sql.Tx, error) {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, store.LockStatement); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}

func openSQLite(path string) (*sql.DB, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if _, err := filepath.EvalSymlinks(abs); err != nil {
		return nil, fmt.Errorf("no SQLite database at %s: %w", path, err)
	}
	// Read-only, and query_only so not even SQLite's own bookkeeping writes.
	u := url.URL{Scheme: "file", Path: filepath.ToSlash(abs), RawQuery: "mode=ro&_pragma=query_only(1)"}
	if !strings.HasPrefix(u.Path, "/") {
		u.Path = "/" + u.Path
	}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

// checkSource refuses a file that is not a Kairo V3 SQLite database.
func checkSource(ctx context.Context, src *sql.Tx) error {
	var version int
	if err := src.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM schema_migrations`).Scan(&version); err != nil {
		return fmt.Errorf("not a Kairo SQLite database: %w", err)
	}
	if version != 3 {
		return fmt.Errorf("the SQLite database has schema version %d; only version 3 can be imported", version)
	}
	var check string
	if err := src.QueryRowContext(ctx, `PRAGMA quick_check`).Scan(&check); err != nil {
		return err
	}
	if check != "ok" {
		return fmt.Errorf("the SQLite database is damaged: %s", check)
	}
	return nil
}

func sqliteTables(ctx context.Context, src *sql.Tx) (map[string]map[string]bool, error) {
	rows, err := src.QueryContext(ctx, `SELECT m.name,p.name FROM sqlite_master m JOIN pragma_table_info(m.name) p WHERE m.type='table'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]map[string]bool{}
	for rows.Next() {
		var t, c string
		if err := rows.Scan(&t, &c); err != nil {
			return nil, err
		}
		if out[t] == nil {
			out[t] = map[string]bool{}
		}
		out[t][c] = true
	}
	return out, rows.Err()
}

// targetTables reads the schema just created: every table but
// schema_version, its columns, key and references, in an order where each
// table comes after the tables it references.
func targetTables(ctx context.Context, tx *sql.Tx) ([]table, error) {
	byName := map[string]*table{}
	var names []string
	rows, err := tx.QueryContext(ctx, `SELECT table_name,column_name,data_type,is_nullable='YES',is_identity='YES' FROM information_schema.columns
		WHERE table_schema=current_schema() AND table_name<>'schema_version' ORDER BY table_name,ordinal_position`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var t string
		var c column
		if err := rows.Scan(&t, &c.name, &c.pgType, &c.nullable, &c.identity); err != nil {
			rows.Close()
			return nil, err
		}
		if byName[t] == nil {
			byName[t] = &table{name: t}
			names = append(names, t)
		}
		byName[t].columns = append(byName[t].columns, c)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	rows, err = tx.QueryContext(ctx, `SELECT c.conrelid::regclass::text,c.contype,COALESCE(c.confrelid::regclass::text,''),
		(SELECT string_agg(a.attname,',' ORDER BY k.i) FROM unnest(c.conkey) WITH ORDINALITY k(n,i) JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.n),
		COALESCE((SELECT string_agg(a.attname,',' ORDER BY k.i) FROM unnest(c.confkey) WITH ORDINALITY k(n,i) JOIN pg_attribute a ON a.attrelid=c.confrelid AND a.attnum=k.n),'')
		FROM pg_constraint c JOIN pg_namespace s ON s.oid=c.connamespace
		WHERE s.nspname=current_schema() AND c.contype IN('p','f')`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var t, kind, ref, colList, refList string
		if err := rows.Scan(&t, &kind, &ref, &colList, &refList); err != nil {
			rows.Close()
			return nil, err
		}
		cols, refCols := strings.Split(colList, ","), strings.Split(refList, ",")
		tb := byName[t]
		if tb == nil {
			continue
		}
		switch {
		case kind == "p":
			tb.key, tb.hasKey = cols, true
		case ref == t:
			if len(cols) != 1 {
				rows.Close()
				return nil, fmt.Errorf("%s references itself by several columns", t)
			}
			tb.selfRef, tb.selfKey = cols[0], refCols[0]
		default:
			tb.parents = append(tb.parents, ref)
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	sort.Strings(names)
	var ordered []table
	placed := map[string]bool{}
	for len(ordered) < len(names) {
		progress := false
		for _, name := range names {
			tb := byName[name]
			if placed[name] {
				continue
			}
			ready := true
			for _, p := range tb.parents {
				if !placed[p] {
					ready = false
				}
			}
			if !ready {
				continue
			}
			if !tb.hasKey {
				for _, c := range tb.columns {
					tb.key = append(tb.key, c.name)
				}
			}
			ordered = append(ordered, *tb)
			placed[name] = true
			progress = true
		}
		if !progress {
			return nil, errors.New("the schema's foreign keys form a cycle")
		}
	}
	return ordered, nil
}

func hasTable(tables []table, name string) bool {
	for _, t := range tables {
		if t.name == name {
			return true
		}
	}
	return false
}

func (t table) hasColumn(name string) bool {
	for _, c := range t.columns {
		if c.name == name {
			return true
		}
	}
	return false
}

func (t table) columnList() string {
	names := make([]string, len(t.columns))
	for i, c := range t.columns {
		names[i] = `"` + c.name + `"`
	}
	return strings.Join(names, ",")
}

// orderBy is the order both checksums read a table in: its key, NULLs first
// (SQLite's order) for a table keyed by every column.
func (t table) orderBy(postgres bool) string {
	parts := make([]string, len(t.key))
	for i, k := range t.key {
		parts[i] = `"` + k + `"`
		if postgres && !t.hasKey {
			parts[i] += " NULLS FIRST"
		}
	}
	return strings.Join(parts, ",")
}

// copyTable copies a table's rows with COPY, converted to the PostgreSQL
// column types. A table that references itself is copied parents first.
func copyTable(ctx context.Context, src *sql.Tx, conn *sql.Conn, t table) (int64, error) {
	rows, err := src.QueryContext(ctx, `SELECT `+t.columnList()+` FROM "`+t.name+`" ORDER BY `+t.orderBy(false))
	if err != nil {
		return 0, err
	}
	var values [][]any
	for rows.Next() {
		raw := make([]any, len(t.columns))
		ptrs := make([]any, len(raw))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			rows.Close()
			return 0, err
		}
		row := make([]any, len(raw))
		for i, c := range t.columns {
			if row[i], err = convert(c, raw[i]); err != nil {
				rows.Close()
				return 0, fmt.Errorf("row %d, column %s: %w", len(values)+1, c.name, err)
			}
		}
		values = append(values, row)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if t.selfRef != "" {
		if values, err = parentsFirst(t, values); err != nil {
			return 0, err
		}
	}
	names := make([]string, len(t.columns))
	for i, c := range t.columns {
		names[i] = c.name
	}
	var copied int64
	err = conn.Raw(func(driverConn any) error {
		pg := driverConn.(*stdlib.Conn).Conn()
		var err error
		copied, err = pg.CopyFrom(ctx, pgx.Identifier{t.name}, names, pgx.CopyFromRows(values))
		return err
	})
	if err != nil {
		return 0, err
	}
	if copied != int64(len(values)) {
		return 0, fmt.Errorf("copied %d of %d rows", copied, len(values))
	}
	return copied, nil
}

// parentsFirst orders the rows of a table that references itself so that a
// row comes after the row it references.
func parentsFirst(t table, rows [][]any) ([][]any, error) {
	keyAt, refAt := -1, -1
	for i, c := range t.columns {
		if c.name == t.selfKey {
			keyAt = i
		}
		if c.name == t.selfRef {
			refAt = i
		}
	}
	placed := map[any]bool{}
	out := make([][]any, 0, len(rows))
	pending := rows
	for len(pending) > 0 {
		var next [][]any
		for _, row := range pending {
			if row[refAt] == nil || placed[row[refAt]] {
				out = append(out, row)
				placed[row[keyAt]] = true
			} else {
				next = append(next, row)
			}
		}
		if len(next) == len(pending) {
			return nil, fmt.Errorf("%d rows of %s reference rows that are missing", len(next), t.name)
		}
		pending = next
	}
	return out, nil
}

// convert turns a SQLite value into the value of a PostgreSQL column.
func convert(c column, v any) (any, error) {
	if v == nil {
		if !c.nullable {
			return nil, errors.New("NULL in a NOT NULL column")
		}
		return nil, nil
	}
	switch c.pgType {
	case "boolean":
		n, ok := v.(int64)
		if !ok || (n != 0 && n != 1) {
			return nil, fmt.Errorf("flag %v is neither 0 nor 1", v)
		}
		return n == 1, nil
	case "bigint":
		switch x := v.(type) {
		case int64:
			return x, nil
		case float64:
			if x == math.Trunc(x) && math.Abs(x) < 1<<53 {
				return int64(x), nil
			}
		case string:
			if n, err := strconv.ParseInt(x, 10, 64); err == nil {
				return n, nil
			}
		}
		return nil, fmt.Errorf("%T %v is not an integer", v, v)
	case "double precision":
		switch x := v.(type) {
		case float64:
			return x, nil
		case int64:
			return float64(x), nil
		}
		return nil, fmt.Errorf("%T %v is not a number", v, v)
	case "text":
		var s string
		switch x := v.(type) {
		case string:
			s = x
		case []byte:
			s = string(x)
		case int64:
			s = strconv.FormatInt(x, 10)
		case float64:
			s = strconv.FormatFloat(x, 'g', -1, 64)
		default:
			return nil, fmt.Errorf("%T is not text", v)
		}
		if !utf8.ValidString(s) {
			return nil, errors.New("text is not UTF-8")
		}
		if strings.ContainsRune(s, 0) {
			return nil, errors.New("text holds a NUL character")
		}
		return s, nil
	}
	return nil, fmt.Errorf("unsupported column type %s", c.pgType)
}

// checksum reads a table in key order and hashes every value, normalized by
// its PostgreSQL column type, so both databases hash to the same sum when
// they hold the same rows. It returns the sum and the number of rows.
func checksum(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, t table, postgres bool) (string, int64, error) {
	rows, err := q.QueryContext(ctx, `SELECT `+t.columnList()+` FROM "`+t.name+`" ORDER BY `+t.orderBy(postgres))
	if err != nil {
		return "", 0, err
	}
	defer rows.Close()
	h := sha256.New()
	var n int64
	raw := make([]any, len(t.columns))
	ptrs := make([]any, len(raw))
	for i := range raw {
		ptrs[i] = &raw[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return "", 0, err
		}
		for i, c := range t.columns {
			s, null, err := normalize(c, raw[i])
			if err != nil {
				return "", 0, fmt.Errorf("column %s: %w", c.name, err)
			}
			var head [9]byte
			if null {
				head[0] = 1
			}
			binary.BigEndian.PutUint64(head[1:], uint64(len(s)))
			h.Write(head[:])
			h.Write([]byte(s))
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// normalize is the text a value hashes as, by the column's PostgreSQL type:
// a flag 0 or 1, an integer in decimal, a real in its shortest exact form,
// text as it is. It reads values as either database driver returns them.
func normalize(c column, v any) (string, bool, error) {
	if v == nil {
		return "", true, nil
	}
	switch c.pgType {
	case "boolean":
		switch x := v.(type) {
		case bool:
			if x {
				return "1", false, nil
			}
			return "0", false, nil
		case int64:
			return strconv.FormatInt(x, 10), false, nil
		}
	case "bigint":
		switch x := v.(type) {
		case int64:
			return strconv.FormatInt(x, 10), false, nil
		case float64:
			return strconv.FormatFloat(x, 'f', -1, 64), false, nil
		case string:
			return x, false, nil
		}
	case "double precision":
		switch x := v.(type) {
		case float64:
			return strconv.FormatFloat(x, 'g', -1, 64), false, nil
		case int64:
			return strconv.FormatFloat(float64(x), 'g', -1, 64), false, nil
		}
	case "text":
		switch x := v.(type) {
		case string:
			return x, false, nil
		case []byte:
			return string(x), false, nil
		case int64:
			return strconv.FormatInt(x, 10), false, nil
		case float64:
			return strconv.FormatFloat(x, 'g', -1, 64), false, nil
		}
	}
	return "", false, fmt.Errorf("%T in a %s column", v, c.pgType)
}

// resetIdentities moves each identity column's sequence past the largest id
// SQLite ever gave out (sqlite_sequence remembers deleted ones), so ids are
// never reused.
func resetIdentities(ctx context.Context, src, tx *sql.Tx, tables []table) error {
	used := map[string]int64{}
	rows, err := src.QueryContext(ctx, `SELECT name,seq FROM sqlite_sequence`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var name string
		var seq int64
		if err := rows.Scan(&name, &seq); err != nil {
			rows.Close()
			return err
		}
		used[name] = seq
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, t := range tables {
		for _, c := range t.columns {
			if !c.identity {
				continue
			}
			var top int64
			if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX("`+c.name+`"),0) FROM "`+t.name+`"`).Scan(&top); err != nil {
				return err
			}
			top = max(top, used[t.name])
			if _, err := tx.ExecContext(ctx, `SELECT setval(pg_get_serial_sequence($1,$2),$3,false)`, t.name, c.name, top+1); err != nil {
				return fmt.Errorf("reset the sequence of %s.%s: %w", t.name, c.name, err)
			}
		}
	}
	return nil
}
