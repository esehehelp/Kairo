package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"kairo/internal/loader"
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

// streamPositions reads where each rank of each stream of decls stood from a
// checkpoint's payload ("streams": {NAME: [{rank, record, sub}...]}): every
// stream there, each with ranks 0..W-1 once each, nothing negative.
func streamPositions(decls []orchestration.StreamDecl, payload []byte) (map[string][]loader.Position, error) {
	var body struct {
		Streams map[string][]loader.Position `json:"streams"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStreamPositions, err)
	}
	out := map[string][]loader.Position{}
	for _, d := range decls {
		ranks, ok := body.Streams[d.Name]
		if !ok || len(ranks) == 0 {
			return nil, fmt.Errorf("%w: the checkpoint says nothing of stream %s (pass it to safe_point)", ErrStreamPositions, d.Name)
		}
		seen := make([]bool, len(ranks))
		for _, p := range ranks {
			if p.Rank < 0 || p.Rank >= len(ranks) || seen[p.Rank] || p.Record < 0 || p.Sub < 0 {
				return nil, fmt.Errorf("%w: stream %s: positions %v are not one each of ranks 0..%d", ErrStreamPositions, d.Name, ranks, len(ranks)-1)
			}
			seen[p.Rank] = true
		}
		out[d.Name] = ranks
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
	positions := map[string][]loader.Position{}
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
		out = append(out, LaunchStream{Name: d.Name, Env: StreamEnv(d.Name), Spec: spec, Positions: positions[d.Name]})
	}
	return out, nil
}

// loaderSpecTx is the loader's spec of a stream as declared: each file with
// the identity it was found to have (none yet: identified as it is read) and
// its record count, when learned.
func loaderSpecTx(ctx context.Context, q queryer, d orchestration.StreamDecl) (loader.Spec, error) {
	spec := loader.Spec{Seed: d.Stream.Shuffle.Seed, Block: d.Stream.Shuffle.Block, ShuffleFiles: d.Stream.Shuffle.Files, MemoryBytes: d.Stream.MemoryBytes}
	if d.Stream.Epochs != nil {
		spec.Epochs = *d.Stream.Epochs
	}
	for _, st := range d.Stream.Stages {
		spec.Stages = append(spec.Stages, loader.Stage{Argv: st.Argv, Workers: st.Workers})
	}
	for _, src := range d.Stream.Sources {
		s := loader.SourceSpec{Name: src.Raw, Weight: src.Weight, Format: src.Format, Field: src.Field, Records: map[string]int64{}}
		for _, f := range d.Files[src.Raw] {
			file := loader.File{Name: f.Name, URL: f.URL, Size: f.Size, SHA256: f.SHA256}
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
				key, err := loader.CountKey(src.Format, src.Field, f.Name)
				if err != nil {
					return spec, err
				}
				var records int64
				err = q.QueryRowContext(ctx, `SELECT records FROM raw_records WHERE sha256=$1 AND framing=$2`, file.SHA256, key).Scan(&records)
				if err == nil {
					s.Records[file.SHA256] = records
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

// StreamReport is what a loader learned: what files were found to be the
// first time they were read (by URL), and how many records files hold.
type StreamReport struct {
	Identities []loader.File  `json:"identities,omitempty"`
	Records    []RecordsCount `json:"records,omitempty"`
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

// RecordStreamReport keeps what the loader of an attempt (its lease, at
// epoch) learned of the files its execution's streams read: the first
// identity of a URL stands (the same again is no error; any other is
// ErrRawChanged, as is one not the published sha256 or size); a record count
// is kept only of a file identified so, read as a source of those streams
// reads it, and the first stands (another is ErrRawChanged).
func (s *Store) RecordStreamReport(ctx context.Context, attemptID, leaseID string, epoch int64, report StreamReport) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := validateEpochTx(ctx, tx, attemptID, leaseID, epoch); err != nil {
		return err
	}
	var executionID string
	if err := tx.QueryRowContext(ctx, `SELECT execution_id FROM attempts WHERE id=$1`, attemptID).Scan(&executionID); err != nil {
		return err
	}
	decls, err := executionStreamsTx(ctx, tx, executionID)
	if err != nil {
		return err
	}
	// each URL the streams read: as declared, and how each source reads it
	type reading struct{ format, field string }
	declared := map[string]orchestration.RawFile{}
	readings := map[string][]reading{}
	for _, d := range decls {
		for _, src := range d.Stream.Sources {
			for _, f := range d.Files[src.Raw] {
				declared[f.URL] = f
				readings[f.URL] = append(readings[f.URL], reading{src.Format, src.Field})
			}
		}
	}
	t := now()
	for _, f := range report.Identities {
		want, ok := declared[f.URL]
		if !ok {
			return fmt.Errorf("%w: %s is no file of a stream this attempt reads", ErrStreamReportRefused, f.URL)
		}
		if f.Size < 0 || f.ChunkBytes < 1 || !sha256Hex.MatchString(f.SHA256) || int64(len(f.Chunks)) != (f.Size+f.ChunkBytes-1)/f.ChunkBytes {
			return fmt.Errorf("identity of %s: a size, a sha256 and a hash per chunk", f.URL)
		}
		for _, c := range f.Chunks {
			if !sha256Hex.MatchString(c) {
				return fmt.Errorf("identity of %s: a chunk hash is not a sha256", f.URL)
			}
		}
		if want.SHA256 != "" && want.SHA256 != f.SHA256 || want.Size > 0 && want.Size != f.Size {
			return fmt.Errorf("%w: %s read as %s (%d bytes), published as %s (%d bytes)", ErrRawChanged, f.URL, f.SHA256, f.Size, want.SHA256, want.Size)
		}
		chunks, _ := json.Marshal(f.Chunks)
		var size, chunkBytes int64
		var sha, keptChunks string
		err := tx.QueryRowContext(ctx, `INSERT INTO raw_identities(url,size,sha256,chunk_bytes,chunks_json,identified_at,attempt_id) VALUES($1,$2,$3,$4,$5,$6,$7)
			ON CONFLICT(url) DO UPDATE SET url=raw_identities.url RETURNING size,sha256,chunk_bytes,chunks_json`, f.URL, f.Size, f.SHA256, f.ChunkBytes, string(chunks), t, attemptID).Scan(&size, &sha, &chunkBytes, &keptChunks)
		if err != nil {
			return err
		}
		if size != f.Size || sha != f.SHA256 || chunkBytes != f.ChunkBytes || keptChunks != string(chunks) {
			return fmt.Errorf("%w: %s was %s (%d bytes) the first time it was read, %s (%d bytes) now", ErrRawChanged, f.URL, sha, size, f.SHA256, f.Size)
		}
	}
	for _, r := range report.Records {
		if r.Records < 0 {
			return fmt.Errorf("a record count of %s: %d", r.URL, r.Records)
		}
		read := false
		for _, x := range readings[r.URL] {
			read = read || x.format == r.Format && x.field == r.Field
		}
		if !read {
			return fmt.Errorf("%w: no stream this attempt reads reads %s as %s (field %q)", ErrStreamReportRefused, r.URL, r.Format, r.Field)
		}
		var sha string
		err := tx.QueryRowContext(ctx, `SELECT sha256 FROM raw_identities WHERE url=$1`, r.URL).Scan(&sha)
		if errors.Is(err, sql.ErrNoRows) || err == nil && sha != r.SHA256 {
			return fmt.Errorf("%w: a record count of %s, not identified as %s", ErrStreamReportRefused, r.URL, r.SHA256)
		}
		if err != nil {
			return err
		}
		key, err := loader.CountKey(r.Format, r.Field, declared[r.URL].Name)
		if err != nil {
			return err
		}
		var kept int64
		if err := tx.QueryRowContext(ctx, `INSERT INTO raw_records(sha256,framing,records,learned_at) VALUES($1,$2,$3,$4)
			ON CONFLICT(sha256,framing) DO UPDATE SET sha256=raw_records.sha256 RETURNING records`, sha, key, r.Records, t).Scan(&kept); err != nil {
			return err
		}
		if kept != r.Records {
			return fmt.Errorf("%w: %s held %d records read as %s, %d now", ErrRawChanged, r.URL, kept, key, r.Records)
		}
	}
	return tx.Commit()
}
