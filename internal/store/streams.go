package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

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

var notEnvChar = regexp.MustCompile(`[^A-Za-z0-9]`)

// StreamEnv is the environment variable of stream name: KAIRO_STREAM_ and
// the name upper-cased, anything not a letter or digit an underscore.
func StreamEnv(name string) string {
	return "KAIRO_STREAM_" + strings.ToUpper(notEnvChar.ReplaceAllString(name, "_"))
}

// framingKey is what a file's record count is kept under: the framing (the
// format without its compression) and the field.
func framingKey(format, field string) string {
	framing, _, _ := strings.Cut(format, ".")
	return framing + "|" + field
}

// launchStreamsTx is the streams the attempt of an execution reads.
func launchStreamsTx(ctx context.Context, q queryer, executionID string) ([]LaunchStream, error) {
	var body, continuation, task sql.NullString
	err := q.QueryRowContext(ctx, `SELECT streams_json,input_continuation_ref,task_scope_id FROM execution_requests WHERE id=$1`, executionID).Scan(&body, &continuation, &task)
	if err != nil || !body.Valid {
		return nil, err
	}
	var decls []orchestration.StreamDecl
	if err := json.Unmarshal([]byte(body.String), &decls); err != nil {
		return nil, fmt.Errorf("streams of execution %s: %w", executionID, err)
	}
	positions := map[string][]loader.Position{}
	if continuation.Valid && task.Valid {
		if positions, err = continuationPositionsTx(ctx, q, task.String, continuation.String); err != nil {
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

// continuationPositionsTx is where each rank of each stream stood when the
// checkpoint ref of the task was published.
func continuationPositionsTx(ctx context.Context, q queryer, taskScopeID, ref string) (map[string][]loader.Position, error) {
	var payload string
	err := q.QueryRowContext(ctx, `SELECT k.payload_json FROM command_acks k JOIN attempts a ON a.id=k.attempt_id JOIN execution_requests e ON e.id=a.execution_id
		WHERE e.task_scope_id=$1 AND k.phase='checkpointed' AND k.payload_json::jsonb->>'continuation_ref'=$2 ORDER BY k.id DESC LIMIT 1`, taskScopeID, ref).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return map[string][]loader.Position{}, nil
	}
	if err != nil {
		return nil, err
	}
	var body struct {
		Streams map[string][]loader.Position `json:"streams"`
	}
	if err := json.Unmarshal([]byte(payload), &body); err != nil || body.Streams == nil {
		return map[string][]loader.Position{}, nil
	}
	return body.Streams, nil
}

// loaderSpecTx is the loader's spec of a stream as declared: each file with
// the identity it was found to have (none yet: identified as it is read) and
// its record count, when learned.
func loaderSpecTx(ctx context.Context, q queryer, d orchestration.StreamDecl) (loader.Spec, error) {
	spec := loader.Spec{Seed: d.Stream.Shuffle.Seed, Block: d.Stream.Shuffle.Block, ShuffleFiles: d.Stream.Shuffle.Files}
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
				var records int64
				err = q.QueryRowContext(ctx, `SELECT records FROM raw_records WHERE sha256=$1 AND framing=$2`, file.SHA256, framingKey(src.Format, src.Field)).Scan(&records)
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

// RecordsCount is the records a file (by sha256) holds under a format and
// field.
type RecordsCount struct {
	SHA256  string `json:"sha256"`
	Format  string `json:"format"`
	Field   string `json:"field,omitempty"`
	Records int64  `json:"records"`
}

// RecordStreamReport keeps what the loader of an attempt learned, of files
// its execution's streams read: the first identity of a URL stands (the same
// again is no error; another is ErrRawChanged), and so does a file's first
// record count.
func (s *Store) RecordStreamReport(ctx context.Context, attemptID string, report StreamReport) error {
	tx, err := s.beginWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var body sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT e.streams_json FROM attempts a JOIN execution_requests e ON e.id=a.execution_id WHERE a.id=$1`, attemptID).Scan(&body); err != nil {
		return err
	}
	var decls []orchestration.StreamDecl
	if body.Valid {
		if err := json.Unmarshal([]byte(body.String), &decls); err != nil {
			return err
		}
	}
	urls := map[string]bool{}
	for _, d := range decls {
		for _, files := range d.Files {
			for _, f := range files {
				urls[f.URL] = true
			}
		}
	}
	t := now()
	for _, f := range report.Identities {
		if !urls[f.URL] {
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
		chunks, _ := json.Marshal(f.Chunks)
		var kept string
		err := tx.QueryRowContext(ctx, `INSERT INTO raw_identities(url,size,sha256,chunk_bytes,chunks_json,identified_at,attempt_id) VALUES($1,$2,$3,$4,$5,$6,$7)
			ON CONFLICT(url) DO UPDATE SET url=raw_identities.url RETURNING sha256`, f.URL, f.Size, f.SHA256, f.ChunkBytes, string(chunks), t, attemptID).Scan(&kept)
		if err != nil {
			return err
		}
		if kept != f.SHA256 {
			return fmt.Errorf("%w: %s was %s the first time it was read, %s now", ErrRawChanged, f.URL, kept, f.SHA256)
		}
	}
	for _, r := range report.Records {
		if !sha256Hex.MatchString(r.SHA256) || r.Records < 0 {
			return fmt.Errorf("a record count of %q: a sha256 and a count", r.SHA256)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO raw_records(sha256,framing,records,learned_at) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
			r.SHA256, framingKey(r.Format, r.Field), r.Records, t); err != nil {
			return err
		}
	}
	return tx.Commit()
}
