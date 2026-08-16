// Package snapshot implements Phase 1 of a migration: extracting the existing
// contents of the source tables and loading them into the target.
//
// Two extraction strategies are supported, and choosing between them is the most
// consequential decision in a migration plan.
//
// The file strategy is what an on-premise bulk unload realistically produces: a
// high-throughput utility writes delimited .dat files, rolling to a new suffixed
// part whenever a part exceeds a size threshold, and those parts are staged to
// object storage and bulk-loaded into the target. It is fast, it uses the
// database's own export path, and it is often the only option when the source is
// a mainframe-adjacent system whose supported export tooling is a batch job.
//
// The stream strategy injects snapshot chunks into the same ordered change
// stream as the CDC events, using watermarks (see window.go) to reconcile the
// two. It is the better default when available, because the snapshot/CDC
// boundary — the single most common source of silent data loss in a migration —
// stops being a thing that has to be defended against and simply stops existing.
//
// Both strategies converge on the same invariant: every row written to the target
// carries the source LSN it was observed at, and every write is fenced on that
// LSN, so a stale snapshot row can never overwrite a fresher change event no
// matter what order they arrive in.
package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// ManifestVersion is bumped whenever the on-disk format changes incompatibly.
const ManifestVersion = 1

// PartState is the lifecycle of a single extracted part.
type PartState string

// Part lifecycle states.
const (
	// PartWriting means the extractor still holds the file open. A part in this
	// state must never be loaded: its tail may be a half-written record.
	PartWriting PartState = "writing"
	// PartSealed means the part is complete and its digest is final. This is the
	// only state from which a part becomes eligible to load, and it is what makes
	// pipelining the extract and the load safe.
	PartSealed PartState = "sealed"
	// PartLoaded means the part has been applied to the target.
	PartLoaded PartState = "loaded"
	// PartFailed means the part exhausted its retry budget.
	PartFailed PartState = "failed"
)

// Part describes one .dat file produced by the extractor.
type Part struct {
	// Index is the zero-based part number, matching the filename suffix.
	Index int `json:"index"`
	// Name is the file name, e.g. "app.accounts.dat.00001".
	Name string `json:"name"`
	// Bytes is the size of the file as stored, after compression if enabled.
	Bytes int64 `json:"bytes"`
	// Rows is the number of records written into the part.
	Rows int64 `json:"rows"`
	// SHA256 is the digest of the stored bytes. It is computed over exactly what
	// lands in object storage, so the same value can be compared against the
	// object's checksum without re-reading and re-deriving anything.
	SHA256 string `json:"sha256"`
	// Compressed reports whether the stored bytes are gzip-encoded.
	Compressed bool `json:"compressed"`
	// ExtractLSN is the source change sequence number this part's contents were
	// consistent as of. Every row loaded from this part is written carrying this
	// LSN, which is what allows the fenced upsert to reject it if a newer change
	// event has already been applied to the same row.
	ExtractLSN uint64 `json:"extract_lsn"`
	// FirstKey and LastKey bound the part's key range in canonical form, so the
	// reconciler can verify exactly the rows a suspect part contained instead of
	// re-checksumming the whole table.
	FirstKey string `json:"first_key,omitempty"`
	LastKey  string `json:"last_key,omitempty"`

	State    PartState `json:"state"`
	SealedAt time.Time `json:"sealed_at,omitempty"`
}

// Eligible reports whether a part may be handed to the loader. Only sealed parts
// qualify; this single check is what prevents the pipelined loader from reading
// a file whose last record is still being written.
func (p Part) Eligible() bool { return p.State == PartSealed && p.SHA256 != "" }

// Manifest is the complete description of one table's extract.
type Manifest struct {
	Version int `json:"version"`

	SourceTable string   `json:"source_table"`
	TargetTable string   `json:"target_table"`
	Columns     []string `json:"columns"`

	// Delimiter and NullSentinel describe the .dat encoding. They are recorded
	// rather than assumed so that a manifest written by one release can still be
	// loaded by another after the defaults change.
	Delimiter    string `json:"delimiter"`
	NullSentinel string `json:"null_sentinel"`
	Quote        string `json:"quote"`

	// ExtractStartLSN and ExtractEndLSN bracket the window during which the
	// extract ran. Every change in this window is also present in the CDC stream,
	// which is precisely why the load must be LSN-fenced rather than blind.
	ExtractStartLSN uint64 `json:"extract_start_lsn"`
	ExtractEndLSN   uint64 `json:"extract_end_lsn"`

	Parts     []Part    `json:"parts"`
	TotalRows int64     `json:"total_rows"`
	CreatedAt time.Time `json:"created_at"`
	SealedAt  time.Time `json:"sealed_at,omitempty"`
	Complete  bool      `json:"complete"`
}

// SealedParts returns the parts eligible for loading, in index order.
func (m *Manifest) SealedParts() []Part {
	out := make([]Part, 0, len(m.Parts))
	for _, p := range m.Parts {
		if p.Eligible() {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out
}

// Part looks a part up by index.
func (m *Manifest) Part(index int) (Part, bool) {
	for _, p := range m.Parts {
		if p.Index == index {
			return p, true
		}
	}
	return Part{}, false
}

// RowsInSealedParts sums the declared row counts of sealed parts. Comparing this
// against the number of rows actually loaded is the cheapest possible integrity
// check and catches a truncated part before reconciliation ever runs.
func (m *Manifest) RowsInSealedParts() int64 {
	var n int64
	for _, p := range m.Parts {
		if p.Eligible() {
			n += p.Rows
		}
	}
	return n
}

// Validate checks a manifest for internal consistency.
func (m *Manifest) Validate() error {
	if m.Version != ManifestVersion {
		return fmt.Errorf("manifest version %d is not supported (want %d)", m.Version, ManifestVersion)
	}
	if m.SourceTable == "" || m.TargetTable == "" {
		return errors.New("manifest is missing table names")
	}
	if len(m.Columns) == 0 {
		return errors.New("manifest declares no columns")
	}
	if m.Delimiter == "" {
		return errors.New("manifest declares no delimiter")
	}

	seen := make(map[int]bool, len(m.Parts))
	var rows int64
	for _, p := range m.Parts {
		if seen[p.Index] {
			return fmt.Errorf("duplicate part index %d", p.Index)
		}
		seen[p.Index] = true
		if p.State == PartSealed {
			if p.SHA256 == "" {
				return fmt.Errorf("part %d is sealed but has no digest", p.Index)
			}
			if p.Rows < 0 {
				return fmt.Errorf("part %d has negative row count", p.Index)
			}
		}
		rows += p.Rows
	}

	// A complete manifest must have every part sealed. Loading from a manifest
	// marked complete while a part is still writing would silently drop rows.
	if m.Complete {
		for _, p := range m.Parts {
			if p.State == PartWriting {
				return fmt.Errorf("manifest is marked complete but part %d is still writing", p.Index)
			}
		}
		if rows != m.TotalRows {
			return fmt.Errorf("manifest total_rows=%d does not match the sum of part rows=%d", m.TotalRows, rows)
		}
		if m.ExtractEndLSN < m.ExtractStartLSN {
			return fmt.Errorf("extract end LSN %d precedes start LSN %d", m.ExtractEndLSN, m.ExtractStartLSN)
		}
	}
	return nil
}

// Write serialises the manifest atomically: it is written to a temporary file
// and renamed into place, so a reader can never observe a half-written manifest
// even if the extractor is killed mid-write.
func (m *Manifest) Write(path string) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding manifest: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("writing manifest: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("installing manifest: %w", err)
	}
	return nil
}

// ReadManifest loads and validates a manifest from disk.
func ReadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path comes from operator config
	if err != nil {
		return nil, fmt.Errorf("reading manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parsing manifest: %w", err)
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return &m, nil
}

// VerifyPart recomputes a part's digest and compares it against the manifest.
//
// This is not belt-and-braces paranoia. A part truncated by a full disk, a
// partial multipart upload, or a retried extract that overwrote a file with a
// shorter one all produce a file that loads without error and silently omits
// rows. The digest turns every one of those into a loud failure before any data
// reaches the target.
func VerifyPart(dir string, p Part) error {
	if p.SHA256 == "" {
		return fmt.Errorf("part %d has no recorded digest", p.Index)
	}
	f, err := os.Open(filepath.Join(dir, p.Name)) //nolint:gosec // path from manifest
	if err != nil {
		return fmt.Errorf("opening part %d: %w", p.Index, err)
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return fmt.Errorf("reading part %d: %w", p.Index, err)
	}
	if n != p.Bytes {
		return fmt.Errorf("part %d is %d bytes on disk but the manifest records %d: the file is truncated or was rewritten",
			p.Index, n, p.Bytes)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != p.SHA256 {
		return fmt.Errorf("part %d digest mismatch: file is %s, manifest records %s", p.Index, got, p.SHA256)
	}
	return nil
}
