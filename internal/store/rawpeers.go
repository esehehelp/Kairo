package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Raw files over the LAN (issue #43): a node's raw cache that does not keep
// a file identified before asks the daemon where else it is kept
// (RawSources) before reading it from upstream, and reads it from another
// node's agent (its transfer endpoint, GET /raw/<sha256>) with a token the
// daemon issued for that one read (raw_reads; AuthorizeRawRead), checking
// every chunk against the file's identity as it would upstream's. A file
// read this way is placed and recorded like one read from upstream.

// RawSource is another node's agent keeping a raw file: its transfer
// endpoint and a token it takes for that file, for a while.
type RawSource struct {
	NodeID string `json:"node_id"`
	Addr   string `json:"addr"`
	Token  string `json:"token"`
}

// rawReadTTL is how long a raw read's token is taken.
const rawReadTTL = time.Hour

// maxRawSources bounds the sources given for one file.
const maxRawSources = 4

// RawSources are the other nodes keeping the file url was identified as
// (ready on a store of theirs, with a transfer endpoint), newest used
// first, each with a token for reading it there; for the raw cache of
// executorID's node, whose leased executions' streams read url. None when
// url was never identified or no other node keeps it.
func (s *Store) RawSources(ctx context.Context, executorID, url string) ([]RawSource, error) {
	wants, _, err := leasedStreamFileTx(ctx, s.db, executorID, url)
	if err != nil {
		return nil, err
	}
	if len(wants) == 0 {
		return nil, fmt.Errorf("%w: %s is no file of a stream an execution leased on the node of %s reads", ErrStreamReportRefused, url, executorID)
	}
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	at := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `DELETE FROM raw_reads WHERE expires_at<$1`, at.Format(time.RFC3339Nano)); err != nil {
		return nil, err
	}
	var sha string
	if err := tx.QueryRowContext(ctx, `SELECT sha256 FROM raw_identities WHERE url=$1`, url).Scan(&sha); errors.Is(err, sql.ErrNoRows) {
		return nil, tx.Commit()
	} else if err != nil {
		return nil, err
	}
	type kept struct{ node, store, root string }
	var found []kept
	err = eachRow(ctx, tx, `SELECT DISTINCT ON (x.node_id) x.node_id,p.store_id,p.root FROM raw_placements p JOIN executors x ON x.id=p.executor_id
		WHERE p.sha256=$1 AND p.state='ready' AND x.node_id<>(SELECT node_id FROM executors WHERE id=$2)
		ORDER BY x.node_id,p.last_used_at DESC`, []any{sha, executorID}, func(rows *sql.Rows) error {
		var k kept
		if err := rows.Scan(&k.node, &k.store, &k.root); err != nil {
			return err
		}
		found = append(found, k)
		return nil
	})
	if err != nil {
		return nil, err
	}
	var out []RawSource
	for _, k := range found {
		if len(out) == maxRawSources {
			break
		}
		addr, err := nodePackAddrTx(ctx, tx, k.node)
		if err != nil {
			return nil, err
		}
		if addr == "" {
			continue
		}
		token, hash, err := newSecret("krr_")
		if err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO raw_reads(token_hash,sha256,node_id,store_id,root,reader_executor_id,created_at,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
			hash, sha, k.node, k.store, k.root, executorID, at.Format(time.RFC3339Nano), at.Add(rawReadTTL).Format(time.RFC3339Nano)); err != nil {
			return nil, err
		}
		out = append(out, RawSource{NodeID: k.node, Addr: addr, Token: token})
	}
	return out, tx.Commit()
}

// AuthorizeRawRead is where nodeID's agent keeps the raw file sha256 that
// token was issued to read there (RawSources): ErrNotFound for a token not
// issued for it, for another node, expired, or of a file no longer kept
// ready there.
func (s *Store) AuthorizeRawRead(ctx context.Context, nodeID, sha256, token string) (PackRead, error) {
	var read PackRead
	var expires string
	err := s.db.QueryRowContext(ctx, `SELECT r.store_id,r.root,r.expires_at FROM raw_reads r
		JOIN raw_placements p ON p.store_id=r.store_id AND p.sha256=r.sha256 AND p.root=r.root AND p.state='ready'
		JOIN executors x ON x.id=p.executor_id AND x.node_id=r.node_id
		WHERE r.token_hash=$1 AND r.sha256=$2 AND r.node_id=$3`, hashToken(token), sha256, nodeID).Scan(&read.StoreID, &read.Root, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return read, ErrNotFound
	}
	if err != nil {
		return read, err
	}
	if at, parseErr := time.Parse(time.RFC3339Nano, expires); parseErr != nil || !time.Now().Before(at) {
		return read, ErrNotFound
	}
	return read, nil
}
