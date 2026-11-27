package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"kairo/internal/loader"
	"kairo/internal/machine"
	"kairo/internal/orchestration"
)

// Streams (issue #43): an execution reads the streams its task declared
// (orchestration.StreamDecl, kept with it), each made for its attempt by a
// loader in the attempt's container (executor/container_linux.go) from the
// spec the launch carries: its sources' files with the identity each was
// found to have the first time a loader read it (raw_identities, by URL:
// every later read is checked against it) and, when learned, how many
// records each holds (raw_records); and, for a continuation, where each rank
// stood when its checkpoint was published (the checkpointed acknowledgement's
// payload: "streams": {NAME: [positions]}).

// LaunchStream is a stream an attempt reads: the environment variable its
// processes find the loader's socket in, the loader's spec and where each
// rank starts (none: the start).
type LaunchStream struct {
	Name      string            `json:"name"`
	Env       string            `json:"env"`
	Spec      loader.Spec       `json:"spec"`
	Positions []loader.Position `json:"positions,omitempty"`
	// World is the ranks the positions are of (0 with none: the first
	// rank's hello says).
	World int `json:"world,omitempty"`
}

// StreamEnv is the environment variable of stream name.
func StreamEnv(name string) string { return orchestration.StreamEnv(name) }

// ErrStreamPositions: a checkpoint of an execution reading streams that does
// not say exactly where each rank of each stream stood.
var ErrStreamPositions = errors.New("stream positions")

// executionStreamsTx is the streams an execution reads (none: nil).
func executionStreamsTx(ctx context.Context, q queryer, executionID string) ([]orchestration.StreamDecl, error) {
	var body sql.NullString
	if err := q.QueryRowContext(ctx, `SELECT streams_json FROM execution_requests WHERE id=$1`, executionID).Scan(&body); err != nil || !body.Valid {
		return nil, err
	}
	var decls []orchestration.StreamDecl
	if err := json.Unmarshal([]byte(body.String), &decls); err != nil {
		return nil, fmt.Errorf("streams of execution %s: %w", executionID, err)
	}
	return decls, nil
}

// StreamCheckpoint is where a stream's ranks stood when a checkpoint was
// published: the ranks (World), the order's version (loader.OrderVersion)
// and each rank's position.
type StreamCheckpoint struct {
	World     int               `json:"world"`
	Order     int               `json:"order"`
	Positions []loader.Position `json:"positions"`
}

// maxPositionRecord bounds a position's record (record*W+rank stays far
// from overflowing) and maxStreamWorld the ranks.
const (
	maxPositionRecord = int64(1) << 48
	maxPositionSub    = int64(1) << 31
	maxStreamWorld    = 1 << 12
)

// streamPositions reads where each rank of each stream of decls stood from a
// checkpoint's payload ("streams": {NAME: {"world", "order", "positions"}}):
// every stream there, of this order, its positions exactly one for each
// rank 0..world-1, within bounds.
func streamPositions(decls []orchestration.StreamDecl, payload []byte) (map[string]StreamCheckpoint, error) {
	var body struct {
		Streams map[string]StreamCheckpoint `json:"streams"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStreamPositions, err)
	}
	// a position says its rank, record and sub: none is taken as 0
	var written struct {
		Streams map[string]struct {
			World     *int `json:"world"`
			Order     *int `json:"order"`
			Positions []struct {
				Rank   *int   `json:"rank"`
				Record *int64 `json:"record"`
				Sub    *int64 `json:"sub"`
			} `json:"positions"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(payload, &written); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStreamPositions, err)
	}
	for name, cp := range written.Streams {
		if cp.World == nil || cp.Order == nil {
			return nil, fmt.Errorf("%w: stream %s: no world or order", ErrStreamPositions, name)
		}
		for _, p := range cp.Positions {
			if p.Rank == nil || p.Record == nil || p.Sub == nil {
				return nil, fmt.Errorf("%w: stream %s: a position without its rank, record and sub", ErrStreamPositions, name)
			}
		}
	}
	out := map[string]StreamCheckpoint{}
	for _, d := range decls {
		cp, ok := body.Streams[d.Name]
		if !ok {
			return nil, fmt.Errorf("%w: the checkpoint says nothing of stream %s (pass it to safe_point)", ErrStreamPositions, d.Name)
		}
		if cp.Order != loader.OrderVersion {
			return nil, fmt.Errorf("%w: stream %s: positions of order %d, not %d", ErrStreamPositions, d.Name, cp.Order, loader.OrderVersion)
		}
		if cp.World < 1 || cp.World > maxStreamWorld || len(cp.Positions) != cp.World {
			return nil, fmt.Errorf("%w: stream %s: %d positions for %d ranks", ErrStreamPositions, d.Name, len(cp.Positions), cp.World)
		}
		seen := make([]bool, cp.World)
		for _, p := range cp.Positions {
			if p.Rank < 0 || p.Rank >= cp.World || seen[p.Rank] || p.Record < 0 || p.Record > maxPositionRecord || p.Sub < 0 || p.Sub > maxPositionSub {
				return nil, fmt.Errorf("%w: stream %s: positions %v are not one each of ranks 0..%d, within bounds", ErrStreamPositions, d.Name, cp.Positions, cp.World-1)
			}
			seen[p.Rank] = true
		}
		out[d.Name] = cp
	}
	return out, nil
}

// checkStreamPositionsTx refuses a checkpointed payload of an execution
// reading streams that does not say where each rank of each stood.
func checkStreamPositionsTx(ctx context.Context, q queryer, executionID string, payload []byte) error {
	decls, err := executionStreamsTx(ctx, q, executionID)
	if err != nil || len(decls) == 0 {
		return err
	}
	_, err = streamPositions(decls, payload)
	return err
}

// launchStreamsTx is the streams the attempt of an execution reads: from
// the start, or, for a continuation, from where the checkpoint it goes on
// from (its command's accepted payload) says each rank stood.
func launchStreamsTx(ctx context.Context, q queryer, executionID string) ([]LaunchStream, error) {
	decls, err := executionStreamsTx(ctx, q, executionID)
	if err != nil || len(decls) == 0 {
		return nil, err
	}
	var continuation, command sql.NullString
	if err := q.QueryRowContext(ctx, `SELECT input_continuation_ref,input_checkpoint_command_id FROM execution_requests WHERE id=$1`, executionID).Scan(&continuation, &command); err != nil {
		return nil, err
	}
	positions := map[string]StreamCheckpoint{}
	if continuation.Valid {
		if !command.Valid {
			return nil, fmt.Errorf("%w: execution %s goes on from %s, not from a checkpoint Kairo knows: where its streams start is not known", ErrStreamPositions, executionID, continuation.String)
		}
		var payload string
		if err := q.QueryRowContext(ctx, `SELECT payload_json FROM commands WHERE id=$1 AND state='checkpointed'`, command.String).Scan(&payload); err != nil {
			return nil, fmt.Errorf("checkpoint %s of execution %s: %w", command.String, executionID, err)
		}
		if positions, err = streamPositions(decls, []byte(payload)); err != nil {
			return nil, err
		}
	}
	out := make([]LaunchStream, 0, len(decls))
	for _, d := range decls {
		spec, err := loaderSpecTx(ctx, q, d)
		if err != nil {
			return nil, err
		}
		cp := positions[d.Name]
		out = append(out, LaunchStream{Name: d.Name, Env: StreamEnv(d.Name), Spec: spec, Positions: cp.Positions, World: cp.World})
	}
	return out, nil
}

// loaderSpecTx is the loader's spec of a stream as declared: each file with
// the identity it was found to have (none yet: identified as it is read) and
// its record count, when learned.
func loaderSpecTx(ctx context.Context, q queryer, d orchestration.StreamDecl) (loader.Spec, error) {
	spec := loader.Spec{Seed: d.Stream.Shuffle.Seed, Block: d.Stream.Shuffle.Block, ShuffleFiles: d.Stream.Shuffle.Files, MemoryBytes: d.Stream.MemoryBytes, WeightBy: d.Stream.WeightBy}
	if d.Stream.Epochs != nil {
		spec.Epochs = *d.Stream.Epochs
	}
	for _, st := range d.Stream.Stages {
		spec.Stages = append(spec.Stages, loader.Stage{Argv: st.Argv, Workers: st.Workers})
	}
	for _, src := range d.Stream.Sources {
		s := loader.SourceSpec{Name: src.Raw, Weight: src.Weight, Format: src.Format, Field: src.Field, Records: map[string]int64{}}
		for _, f := range d.Files[src.Raw] {
			file := loader.File{Name: f.Name, URL: f.URL, Size: f.Size, SHA256: f.SHA256, PublishedSHA256: f.SHA256}
			if f.SizeKnown {
				size := f.Size
				file.PublishedSize = &size
			}
			var chunks string
			err := q.QueryRowContext(ctx, `SELECT size,sha256,chunk_bytes,chunks_json FROM raw_identities WHERE url=$1`, f.URL).Scan(&file.Size, &file.SHA256, &file.ChunkBytes, &chunks)
			switch {
			case errors.Is(err, sql.ErrNoRows):
			case err != nil:
				return spec, err
			default:
				if err := json.Unmarshal([]byte(chunks), &file.Chunks); err != nil {
					return spec, fmt.Errorf("identity of %s: %w", f.URL, err)
				}
				// a file not what was published is read (and refused), never
				// passed over by a count
				if file.AsPublished() != "" {
					break
				}
				key, err := loader.CountKey(src.Format, src.Field, f.Name)
				if err != nil {
					return spec, err
				}
				var records int64
				err = q.QueryRowContext(ctx, `SELECT records FROM raw_records WHERE sha256=$1 AND framing=$2`, file.SHA256, key).Scan(&records)
				if err == nil {
					recordsKey, _ := loader.RecordsKey(file.SHA256, src.Format, src.Field, f.Name)
					s.Records[recordsKey] = records
				} else if !errors.Is(err, sql.ErrNoRows) {
					return spec, err
				}
			}
			s.Files = append(s.Files, file)
		}
		spec.Sources = append(spec.Sources, s)
	}
	return spec, nil
}

// ErrStreamReportRefused: a loader reported of a file its attempt's
// streams do not read.
var ErrStreamReportRefused = errors.New("stream report refused")

// ErrRawChanged: a raw file was found to be other than it was the first time
// it was read.
var ErrRawChanged = errors.New("raw data changed")

// ErrRawEvicting: a raw file is being evicted from the store it would be
// placed on.
var ErrRawEvicting = errors.New("raw file being evicted")

// leasedStreamFileTx is every declaration of url among the streams of the
// executions leased on executorID's node (live: reserved to releasing), on
// any of its executors: a node's raw cache serves them all and reports as
// one of them. And how their sources read it (format, field); none when
// none reads it.
func leasedStreamFileTx(ctx context.Context, q queryer, executorID, url string) ([]orchestration.RawFile, []RecordsCount, error) {
	var wants []orchestration.RawFile
	var readings []RecordsCount
	err := eachRow(ctx, q, `SELECT e.streams_json FROM leases l JOIN execution_requests e ON e.id=l.execution_id JOIN executors x ON x.id=l.executor_id
		WHERE x.node_id=(SELECT node_id FROM executors WHERE id=$1) AND l.state IN('reserved','prepared','active','releasing') AND e.streams_json IS NOT NULL`, []any{executorID}, func(rows *sql.Rows) error {
		var body string
		if err := rows.Scan(&body); err != nil {
			return err
		}
		var decls []orchestration.StreamDecl
		if err := json.Unmarshal([]byte(body), &decls); err != nil {
			return err
		}
		for _, d := range decls {
			for _, src := range d.Stream.Sources {
				for _, f := range d.Files[src.Raw] {
					if f.URL == url {
						wants = append(wants, f)
						readings = append(readings, RecordsCount{URL: url, Format: src.Format, Field: src.Field})
					}
				}
			}
		}
		return nil
	})
	return wants, readings, err
}

// RawIdentity is what url was found to be the first time it was read, for
// the agent of executorID (whose node's leased executions' streams read
// it).
func (s *Store) RawIdentity(ctx context.Context, executorID, url string) (loader.File, bool, error) {
	wants, _, err := leasedStreamFileTx(ctx, s.db, executorID, url)
	if err != nil {
		return loader.File{}, false, err
	}
	if len(wants) == 0 {
		return loader.File{}, false, fmt.Errorf("%w: %s is no file of a stream an execution leased on the node of %s reads", ErrStreamReportRefused, url, executorID)
	}
	f := loader.File{URL: url}
	var chunks string
	err = s.db.QueryRowContext(ctx, `SELECT size,sha256,chunk_bytes,chunks_json FROM raw_identities WHERE url=$1`, url).Scan(&f.Size, &f.SHA256, &f.ChunkBytes, &chunks)
	if errors.Is(err, sql.ErrNoRows) {
		return loader.File{}, false, nil
	}
	if err != nil {
		return loader.File{}, false, err
	}
	if err := json.Unmarshal([]byte(chunks), &f.Chunks); err != nil {
		return loader.File{}, false, err
	}
	return f, true, nil
}

// RecordsCount is the records a file (its URL, and its sha256) holds read
// as a source of a stream reads it (its format and field).
type RecordsCount struct {
	URL     string `json:"url"`
	SHA256  string `json:"sha256"`
	Format  string `json:"format"`
	Field   string `json:"field,omitempty"`
	Records int64  `json:"records"`
}

// RawRecords keeps how many records a raw file holds, as the agent of
// executorID counted them in the file it keeps: only of a file identified
// so, read so by a source of a stream an execution leased there reads; the
// first count stands (another is ErrRawChanged).
func (s *Store) RawRecords(ctx context.Context, executorID string, r RecordsCount) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	wants, readings, err := leasedStreamFileTx(ctx, tx, executorID, r.URL)
	if err != nil {
		return err
	}
	found := len(wants) > 0
	read := false
	for _, x := range readings {
		read = read || x.Format == r.Format && x.Field == r.Field
	}
	if !found || !read || r.Records < 0 {
		return fmt.Errorf("%w: no stream an execution leased on %s reads %s as %s (field %q)", ErrStreamReportRefused, executorID, r.URL, r.Format, r.Field)
	}
	var sha string
	err = tx.QueryRowContext(ctx, `SELECT sha256 FROM raw_identities WHERE url=$1`, r.URL).Scan(&sha)
	if errors.Is(err, sql.ErrNoRows) || err == nil && sha != r.SHA256 {
		return fmt.Errorf("%w: a record count of %s, not identified as %s", ErrStreamReportRefused, r.URL, r.SHA256)
	}
	if err != nil {
		return err
	}
	key, err := loader.CountKey(r.Format, r.Field, wants[0].Name)
	if err != nil {
		return err
	}
	var kept int64
	if err := tx.QueryRowContext(ctx, `INSERT INTO raw_records(sha256,framing,records,learned_at) VALUES($1,$2,$3,$4)
		ON CONFLICT(sha256,framing) DO UPDATE SET sha256=raw_records.sha256 RETURNING records`, sha, key, r.Records, now()).Scan(&kept); err != nil {
		return err
	}
	if kept != r.Records {
		return fmt.Errorf("%w: %s held %d records read as %s, %d now", ErrRawChanged, r.URL, kept, key, r.Records)
	}
	return tx.Commit()
}

// RawPlaced is a raw file the agent of executorID fetched, checked and kept
// on volatile store storeID at root (<root>/raw/<sha256>), or handed a loader
// again: its identity is recorded as a loader's would be (the first of its
// URL stands; another is ErrRawChanged), the file kept as on the store (last
// used now). With no store, a file too large to keep: its identity only.
// The URL must be a file of a stream an execution leased on executorID
// reads.
func (s *Store) RawPlaced(ctx context.Context, executorID, storeID, root string, f loader.File) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	me, err := executorInfoTx(ctx, tx, executorID)
	if err != nil {
		return err
	}
	declared, err := declaredStores(ctx, tx)
	if err != nil {
		return err
	}
	served := storeID == ""
	for _, d := range declared {
		served = served || d.ID == storeID && d.Class == machine.StoreVolatile && d.Root == root && me.serves(d, executorID)
	}
	if !served {
		return fmt.Errorf("%w: executor %s serves no volatile store %s at %s", ErrStreamReportRefused, executorID, storeID, root)
	}
	wants, _, err := leasedStreamFileTx(ctx, tx, executorID, f.URL)
	if err != nil {
		return err
	}
	if len(wants) == 0 {
		return fmt.Errorf("%w: %s is no file of a stream an execution leased on the node of %s reads", ErrStreamReportRefused, f.URL, executorID)
	}
	if err := keepIdentityTx(ctx, tx, wants, f, ""); err != nil {
		return err
	}
	if storeID == "" {
		return tx.Commit()
	}
	// a file being evicted is not placed again: the agent's eviction may
	// remove it any moment (one already there is handed as it is: a link to
	// it keeps it)
	var state string
	err = tx.QueryRowContext(ctx, `SELECT state FROM raw_placements WHERE store_id=$1 AND sha256=$2 FOR UPDATE`, storeID, f.SHA256).Scan(&state)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if state == "evicting" {
		if err := tx.Commit(); err != nil { // the identity stands
			return err
		}
		return fmt.Errorf("%w: %s on %s", ErrRawEvicting, f.SHA256, storeID)
	}
	t := now()
	if _, err := tx.ExecContext(ctx, `INSERT INTO raw_placements(store_id,root,sha256,executor_id,bytes,state,created_at,last_used_at) VALUES($1,$2,$3,$4,$5,'ready',$6,$7)
		ON CONFLICT(store_id,sha256) DO UPDATE SET root=excluded.root,executor_id=excluded.executor_id,bytes=excluded.bytes,last_used_at=excluded.last_used_at`,
		storeID, root, f.SHA256, executorID, f.Size, t, t); err != nil {
		return err
	}
	return tx.Commit()
}

// keepIdentityTx records f as what its URL was found to be (declared as
// wants), unless another identity of it stands (ErrRawChanged), or f is
// not what any declaration published of it: each declaration is checked
// against it by its own readers.
func keepIdentityTx(ctx context.Context, tx *sql.Tx, wants []orchestration.RawFile, f loader.File, attemptID string) error {
	if f.Size < 0 || f.ChunkBytes < 1 || !sha256Hex.MatchString(f.SHA256) || int64(len(f.Chunks)) != (f.Size+f.ChunkBytes-1)/f.ChunkBytes {
		return fmt.Errorf("identity of %s: a size, a sha256 and a hash per chunk", f.URL)
	}
	for _, c := range f.Chunks {
		if !sha256Hex.MatchString(c) {
			return fmt.Errorf("identity of %s: a chunk hash is not a sha256", f.URL)
		}
	}
	accepted := len(wants) == 0
	for _, want := range wants {
		accepted = accepted || (want.SHA256 == "" || want.SHA256 == f.SHA256) && (!want.SizeKnown || want.Size == f.Size)
	}
	if !accepted {
		return fmt.Errorf("%w: %s read as %s (%d bytes), published otherwise (as %s, %d bytes)", ErrRawChanged, f.URL, f.SHA256, f.Size, wants[0].SHA256, wants[0].Size)
	}
	chunks, _ := json.Marshal(f.Chunks)
	var size, chunkBytes int64
	var sha, keptChunks string
	err := tx.QueryRowContext(ctx, `INSERT INTO raw_identities(url,size,sha256,chunk_bytes,chunks_json,identified_at,attempt_id) VALUES($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT(url) DO UPDATE SET url=raw_identities.url RETURNING size,sha256,chunk_bytes,chunks_json`, f.URL, f.Size, f.SHA256, f.ChunkBytes, string(chunks), now(), nullable(attemptID)).Scan(&size, &sha, &chunkBytes, &keptChunks)
	if err != nil {
		return err
	}
	if size != f.Size || sha != f.SHA256 || chunkBytes != f.ChunkBytes || keptChunks != string(chunks) {
		return fmt.Errorf("%w: %s was %s (%d bytes) the first time it was read, %s (%d bytes) now", ErrRawChanged, f.URL, sha, size, f.SHA256, f.Size)
	}
	return nil
}
