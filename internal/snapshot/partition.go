package snapshot

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/udaykishore-resu/db-migration-platform/internal/model"
)

// DefaultMaxPartBytes is the roll threshold for a .dat part.
//
// The number is a balance between two failure modes. Parts that are too large
// make retries expensive — a failure at 99% of a 50 GB part throws away 50 GB of
// work — and delay the start of the pipelined load. Parts that are too small
// multiply per-part overhead: object storage requests, staging table churn, and
// manifest size. A few gigabytes puts a failed retry at a couple of minutes while
// keeping the part count per table in the low thousands.
const DefaultMaxPartBytes int64 = 2 << 30 // 2 GiB

// DefaultMaxPartRows caps rows per part independently of size, so a table of
// very narrow rows still produces parts that load in bounded time.
const DefaultMaxPartRows int64 = 20_000_000

// DefaultNullSentinel is the token written for a SQL NULL.
//
// This matters more than it looks. CSV cannot distinguish an empty string from a
// NULL, so a migration that writes both as "" turns every NULL in a nullable text
// column into an empty string on the target — a difference that no application
// notices until a `WHERE col IS NULL` query silently returns nothing. Both
// Postgres COPY and MySQL LOAD DATA understand \N, so the same files load into
// either engine without transformation.
const DefaultNullSentinel = `\N`

// PartWriterConfig configures the extractor's output.
type PartWriterConfig struct {
	// Dir is where parts and the manifest are written.
	Dir string
	// Spec identifies the table and its column order.
	Spec model.TableSpec
	// MaxPartBytes rolls to a new part once uncompressed output exceeds this.
	MaxPartBytes int64
	// MaxPartRows rolls to a new part once this many rows have been written.
	MaxPartRows int64
	// Compress gzips each part. Worth it whenever parts cross a network.
	Compress bool
	// Delimiter separates fields. Defaults to a comma.
	Delimiter rune
	// NullSentinel represents SQL NULL. Defaults to DefaultNullSentinel.
	NullSentinel string
	// ExtractStartLSN is the source LSN at which the extract began.
	ExtractStartLSN uint64

	// OnSeal is called as soon as a part is complete and its digest is final.
	//
	// This callback is the whole reason the extract and the load can be
	// pipelined: rather than waiting for every part before loading any, the
	// loader is handed each part the moment it becomes safe to read. On a large
	// table this roughly halves wall-clock time and narrows the window in which
	// the snapshot and the change stream can disagree.
	OnSeal func(Part) error
}

func (c *PartWriterConfig) applyDefaults() {
	if c.MaxPartBytes <= 0 {
		c.MaxPartBytes = DefaultMaxPartBytes
	}
	if c.MaxPartRows <= 0 {
		c.MaxPartRows = DefaultMaxPartRows
	}
	if c.Delimiter == 0 {
		c.Delimiter = ','
	}
	if c.NullSentinel == "" {
		c.NullSentinel = DefaultNullSentinel
	}
}

// PartWriter writes a table's extract as size-bounded, suffixed .dat parts and
// maintains the manifest that describes them.
type PartWriter struct {
	cfg     PartWriterConfig
	columns []string

	cur      *openPart
	parts    []Part
	rows     int64
	nextIdx  int
	manifest string
	closed   bool
}

type openPart struct {
	index    int
	name     string
	file     *os.File
	gz       *gzip.Writer
	csv      *csv.Writer
	counter  *countingWriter
	digest   hash.Hash
	rows     int64
	pending  int64
	firstKey string
	lastKey  string
}

// flushThresholdBytes bounds how far the byte counter can lag reality.
const flushThresholdBytes int64 = 64 << 10

// countingWriter tracks uncompressed bytes so that the roll decision does not
// depend on how well a particular part happens to compress. A size-based roll
// that measured compressed bytes would produce wildly uneven parts as the
// compression ratio varied across the table.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// NewPartWriter creates the output directory and prepares the first part.
func NewPartWriter(cfg PartWriterConfig) (*PartWriter, error) {
	cfg.applyDefaults()
	if cfg.Dir == "" {
		return nil, errors.New("snapshot: part writer requires an output directory")
	}
	if err := cfg.Spec.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.Dir, 0o750); err != nil {
		return nil, fmt.Errorf("snapshot: creating output directory: %w", err)
	}

	w := &PartWriter{
		cfg:      cfg,
		columns:  cfg.Spec.ColumnNames(),
		manifest: filepath.Join(cfg.Dir, manifestName(cfg.Spec.Source)),
	}
	if err := w.roll(); err != nil {
		return nil, err
	}
	if err := w.writeManifest(false); err != nil {
		return nil, err
	}
	return w, nil
}

// manifestName derives a filesystem-safe manifest name from a table reference.
func manifestName(t model.TableRef) string { return slug(t) + ".manifest.json" }

func slug(t model.TableRef) string {
	s := t.String()
	s = strings.ReplaceAll(s, string(filepath.Separator), "_")
	s = strings.ReplaceAll(s, " ", "_")
	return s
}

// partName renders the suffixed part filename. Zero-padding to five digits keeps
// lexical order equal to numeric order, so a plain `ls` or an S3 prefix listing
// returns the parts in load order without any client-side sorting.
func (w *PartWriter) partName(index int) string {
	name := fmt.Sprintf("%s.dat.%05d", slug(w.cfg.Spec.Source), index)
	if w.cfg.Compress {
		name += ".gz"
	}
	return name
}

// roll seals the current part, if any, and opens the next one.
func (w *PartWriter) roll() error {
	if w.cur != nil {
		if err := w.seal(); err != nil {
			return err
		}
	}

	idx := w.nextIdx
	w.nextIdx++
	name := w.partName(idx)
	path := filepath.Join(w.cfg.Dir, name)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // path is derived from operator config
	if err != nil {
		return fmt.Errorf("snapshot: creating part %d: %w", idx, err)
	}

	digest := sha256.New()
	// The digest covers exactly the bytes that land on disk and in object
	// storage, so it can be compared against a stored object checksum directly.
	stored := io.MultiWriter(f, digest)

	p := &openPart{index: idx, name: name, file: f, digest: digest}
	var sink io.Writer = stored
	if w.cfg.Compress {
		p.gz = gzip.NewWriter(stored)
		sink = p.gz
	}
	p.counter = &countingWriter{w: sink}
	p.csv = csv.NewWriter(p.counter)
	p.csv.Comma = w.cfg.Delimiter

	w.cur = p
	w.parts = append(w.parts, Part{
		Index:      idx,
		Name:       name,
		Compressed: w.cfg.Compress,
		ExtractLSN: w.cfg.ExtractStartLSN,
		State:      PartWriting,
	})
	return nil
}

// seal finalises the current part and notifies the loader.
func (w *PartWriter) seal() error {
	p := w.cur
	if p == nil {
		return nil
	}
	w.cur = nil

	p.csv.Flush()
	if err := p.csv.Error(); err != nil {
		_ = p.file.Close()
		return fmt.Errorf("snapshot: flushing part %d: %w", p.index, err)
	}
	if p.gz != nil {
		if err := p.gz.Close(); err != nil {
			_ = p.file.Close()
			return fmt.Errorf("snapshot: closing gzip for part %d: %w", p.index, err)
		}
	}
	// fsync before the part is advertised as sealed. Without it, a host failure
	// between the callback and the page cache flush yields a part the manifest
	// swears is complete and the filesystem disagrees about.
	if err := p.file.Sync(); err != nil {
		_ = p.file.Close()
		return fmt.Errorf("snapshot: syncing part %d: %w", p.index, err)
	}
	info, err := p.file.Stat()
	if err != nil {
		_ = p.file.Close()
		return fmt.Errorf("snapshot: stat part %d: %w", p.index, err)
	}
	if err := p.file.Close(); err != nil {
		return fmt.Errorf("snapshot: closing part %d: %w", p.index, err)
	}

	sealed := Part{
		Index:      p.index,
		Name:       p.name,
		Bytes:      info.Size(),
		Rows:       p.rows,
		SHA256:     hex.EncodeToString(p.digest.Sum(nil)),
		Compressed: w.cfg.Compress,
		ExtractLSN: w.cfg.ExtractStartLSN,
		FirstKey:   p.firstKey,
		LastKey:    p.lastKey,
		State:      PartSealed,
		SealedAt:   time.Now().UTC(),
	}
	for i := range w.parts {
		if w.parts[i].Index == p.index {
			w.parts[i] = sealed
			break
		}
	}

	// Persist the manifest before announcing the part. If the process dies
	// between the two, the loader sees a sealed part in a manifest it can trust;
	// the reverse order would announce a part that no manifest records.
	if err := w.writeManifest(false); err != nil {
		return err
	}
	if w.cfg.OnSeal != nil {
		if err := w.cfg.OnSeal(sealed); err != nil {
			return fmt.Errorf("snapshot: seal callback for part %d: %w", p.index, err)
		}
	}
	return nil
}

// WriteRow appends one row. Values must be in the spec's declared column order.
// The optional key is the canonical row key, recorded as the part's range bound
// so that a suspect part can be reconciled without rescanning the whole table.
func (w *PartWriter) WriteRow(key string, values []any) error {
	if w.closed {
		return errors.New("snapshot: part writer is closed")
	}
	if len(values) != len(w.columns) {
		return fmt.Errorf("snapshot: row has %d values but the table declares %d columns", len(values), len(w.columns))
	}

	record := make([]string, len(values))
	var est int64
	for i, v := range values {
		record[i] = w.encode(v)
		est += int64(len(record[i])) + 1
	}
	if err := w.cur.csv.Write(record); err != nil {
		return fmt.Errorf("snapshot: writing row to part %d: %w", w.cur.index, err)
	}

	// csv.Writer buffers, so the byte counter only advances on flush. Flushing
	// per row would cost a syscall per row; never flushing would make the roll
	// decision blind. Track an estimate of the unflushed bytes and flush once it
	// grows past a page-ish threshold, which keeps the roll accurate to within
	// that threshold at negligible cost.
	w.cur.pending += est
	if w.cur.pending >= flushThresholdBytes {
		w.cur.csv.Flush()
		if err := w.cur.csv.Error(); err != nil {
			return fmt.Errorf("snapshot: flushing part %d: %w", w.cur.index, err)
		}
		w.cur.pending = 0
	}

	w.cur.rows++
	w.rows++
	if key != "" {
		if w.cur.firstKey == "" {
			w.cur.firstKey = key
		}
		w.cur.lastKey = key
	}

	if w.cur.counter.n+w.cur.pending >= w.cfg.MaxPartBytes || w.cur.rows >= w.cfg.MaxPartRows {
		return w.roll()
	}
	return nil
}

// encode renders one value into its .dat representation. NULL becomes the
// sentinel; everything else uses the same canonical normalisation the row-key and
// reconciliation layers use, so a value never means one thing in the extract and
// something subtly different when it is checksummed.
func (w *PartWriter) encode(v any) string {
	if v == nil {
		return w.cfg.NullSentinel
	}
	if s, ok := v.(string); ok {
		// A literal that happens to equal the sentinel must be escaped, or a
		// customer whose surname is the sentinel string becomes a NULL.
		if s == w.cfg.NullSentinel {
			return `\` + s
		}
		return s
	}
	return model.CanonicalValue(v)
}

// Close seals the final part and marks the manifest complete. extractEndLSN is
// the source LSN at which the extract finished; together with the start LSN it
// brackets the window whose changes are also present in the change stream.
func (w *PartWriter) Close(extractEndLSN uint64) (*Manifest, error) {
	if w.closed {
		return nil, errors.New("snapshot: part writer already closed")
	}
	// Drop a trailing empty part rather than sealing a zero-row file: an empty
	// part is not wrong, but it puts a pointless entry in every manifest and in
	// every operator's mental model of the extract.
	if w.cur != nil && w.cur.rows == 0 && len(w.parts) > 1 {
		idx := w.cur.index
		path := filepath.Join(w.cfg.Dir, w.cur.name)
		if w.cur.gz != nil {
			_ = w.cur.gz.Close()
		}
		_ = w.cur.file.Close()
		_ = os.Remove(path)
		w.cur = nil
		for i := range w.parts {
			if w.parts[i].Index == idx {
				w.parts = append(w.parts[:i], w.parts[i+1:]...)
				break
			}
		}
	} else if err := w.seal(); err != nil {
		return nil, err
	}

	w.closed = true
	m := w.buildManifest(true)
	m.ExtractEndLSN = extractEndLSN
	if err := m.Validate(); err != nil {
		return nil, err
	}
	if err := m.Write(w.manifest); err != nil {
		return nil, err
	}
	return m, nil
}

// ManifestPath reports where the manifest is written.
func (w *PartWriter) ManifestPath() string { return w.manifest }

// Rows reports how many rows have been written so far.
func (w *PartWriter) Rows() int64 { return w.rows }

func (w *PartWriter) writeManifest(complete bool) error {
	return w.buildManifest(complete).Write(w.manifest)
}

func (w *PartWriter) buildManifest(complete bool) *Manifest {
	m := &Manifest{
		Version:         ManifestVersion,
		SourceTable:     w.cfg.Spec.Source.String(),
		TargetTable:     w.cfg.Spec.Target.String(),
		Columns:         w.columns,
		Delimiter:       string(w.cfg.Delimiter),
		NullSentinel:    w.cfg.NullSentinel,
		Quote:           `"`,
		ExtractStartLSN: w.cfg.ExtractStartLSN,
		Parts:           append([]Part(nil), w.parts...),
		TotalRows:       w.rows,
		CreatedAt:       time.Now().UTC(),
		Complete:        complete,
	}
	if complete {
		m.SealedAt = time.Now().UTC()
	}
	return m
}
