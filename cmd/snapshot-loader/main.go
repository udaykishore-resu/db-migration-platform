// Command snapshot-loader applies extracted .dat parts to the target.
//
// It watches the extract directory for manifests and loads every sealed part
// through a bounded worker pool. Because a part becomes loadable the moment it is
// sealed, the loader runs concurrently with the extract rather than after it —
// which roughly halves wall-clock time on a large table and narrows the window in
// which the snapshot and the change stream can disagree about a row.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/udaykishore-resu/db-migration-platform/internal/app"
	"github.com/udaykishore-resu/db-migration-platform/internal/dialect"
	"github.com/udaykishore-resu/db-migration-platform/internal/model"
	"github.com/udaykishore-resu/db-migration-platform/internal/obs"
	"github.com/udaykishore-resu/db-migration-platform/internal/retryx"
	"github.com/udaykishore-resu/db-migration-platform/internal/snapshot"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "snapshot-loader: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	flags := app.ParseFlags()

	a, err := app.Bootstrap(ctx, "snapshot-loader", flags, nil)
	if err != nil {
		return err
	}

	reg := a.Metrics
	partsLoaded := reg.Counter("migration_parts_loaded_total", "Parts merged into the target", "table")
	partsFailed := reg.Counter("migration_parts_failed_total", "Parts that failed to load", "table")
	rowsLoaded := reg.Counter("migration_snapshot_rows_loaded_total", "Rows merged from parts", "table")
	inflight := reg.Gauge("migration_parts_inflight", "Parts currently loading")
	loadSeconds := reg.Histogram("migration_part_load_seconds", "Time to load one part", nil, "table")

	if flags.DryRun {
		a.Log.InfoContext(ctx, "dry run complete: configuration, key material and target database all reachable")
		a.Close()
		return nil
	}

	l := &loader{
		app:         a,
		concurrency: a.Cfg.Extract.LoaderConcurrency,
		dir:         a.Cfg.Storage.LocalDir,
		partsLoaded: partsLoaded,
		partsFailed: partsFailed,
		rowsLoaded:  rowsLoaded,
		inflight:    inflight,
		loadSeconds: loadSeconds,
	}

	return a.Run(ctx, func(ctx context.Context) error {
		ctx = obs.WithComponent(ctx, "loader")
		for {
			if err := ctx.Err(); err != nil {
				return nil
			}
			done, err := l.pass(ctx)
			if err != nil {
				a.Log.ErrorContext(ctx, "load pass failed", slog.String("error", err.Error()))
			}
			if flags.Once || (done && flags.Once) {
				return nil
			}
			// Poll rather than watch the filesystem: parts arrive every few
			// minutes at most, an inotify watch adds a platform dependency, and a
			// missed event would stall the load silently.
			if !retryx.Sleep(ctx, 5*time.Second) {
				return nil
			}
		}
	})
}

type loader struct {
	app         *app.App
	concurrency int
	dir         string

	partsLoaded *obs.CounterVec
	partsFailed *obs.CounterVec
	rowsLoaded  *obs.CounterVec
	inflight    *obs.GaugeVec
	loadSeconds *obs.HistogramVec
}

// pass loads every sealed, unloaded part currently visible.
func (l *loader) pass(ctx context.Context) (bool, error) {
	manifests, err := filepath.Glob(filepath.Join(l.dir, "*.manifest.json"))
	if err != nil {
		return false, fmt.Errorf("scanning %s: %w", l.dir, err)
	}
	sort.Strings(manifests)

	allComplete := true
	for _, path := range manifests {
		m, err := snapshot.ReadManifest(path)
		if err != nil {
			// A manifest being rewritten atomically can momentarily fail to
			// parse; that is not an error worth failing the pass over.
			l.app.Log.DebugContext(ctx, "skipping unreadable manifest",
				slog.String("path", path), slog.String("error", err.Error()))
			allComplete = false
			continue
		}
		if !m.Complete {
			allComplete = false
		}
		if err := l.loadManifest(ctx, m); err != nil {
			return false, err
		}
	}
	return allComplete, nil
}

func (l *loader) loadManifest(ctx context.Context, m *snapshot.Manifest) error {
	spec, ok := l.app.Cfg.Plan.Table(model.ParseTableRef(m.SourceTable))
	if !ok {
		return fmt.Errorf("manifest refers to %s, which is not in the migration plan", m.SourceTable)
	}

	parts := m.SealedParts()
	sem := make(chan struct{}, l.concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for _, p := range parts {
		loaded, err := l.app.Store.PartLoaded(ctx, spec.Source, p.Index)
		if err != nil {
			return err
		}
		if loaded {
			continue
		}

		wg.Add(1)
		go func(p snapshot.Part) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()

			l.inflight.Inc()
			defer l.inflight.Dec()

			start := time.Now()
			if err := l.loadPart(ctx, spec, m, p); err != nil {
				l.partsFailed.Inc(spec.Source.String())
				l.app.Log.ErrorContext(ctx, "part failed to load",
					slog.String("table", spec.Source.String()),
					slog.Int("part", p.Index),
					slog.String("error", err.Error()))
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			l.loadSeconds.Observe(time.Since(start).Seconds(), spec.Source.String())
			l.partsLoaded.Inc(spec.Source.String())
			l.rowsLoaded.Add(float64(p.Rows), spec.Source.String())
		}(p)
	}
	wg.Wait()
	return firstErr
}

// loadPart verifies, stages and merges a single part.
func (l *loader) loadPart(ctx context.Context, spec model.TableSpec, m *snapshot.Manifest, p snapshot.Part) error {
	// Verify before touching the database. A truncated part loads without error
	// and silently omits rows; the digest turns that into a loud failure while it
	// is still cheap to fix.
	if err := snapshot.VerifyPart(l.dir, p); err != nil {
		return err
	}

	staging := model.TableRef{Schema: dialect.ControlSchema, Name: "stg_" + spec.Target.Name}
	d := l.app.Dialect

	tx, err := l.app.Target.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	for _, stmt := range d.BulkLoadSessionSettings() {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("applying bulk-load session setting: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, d.CreateStagingTable(spec.Target, staging, m.Columns)); err != nil {
		return fmt.Errorf("creating staging table: %w", err)
	}

	src := dialect.S3Source{
		Bucket:       l.app.Cfg.Storage.Bucket,
		Key:          filepath.Join(l.app.Cfg.Storage.Prefix, p.Name),
		Region:       l.app.Cfg.Storage.Region,
		Compressed:   p.Compressed,
		Delimiter:    m.Delimiter,
		NullSentinel: m.NullSentinel,
	}
	importSQL, importArgs := d.BulkImport(staging, m.Columns, src)
	if _, err := tx.ExecContext(ctx, importSQL, importArgs...); err != nil {
		return fmt.Errorf("importing %s: %w", src.URI(), err)
	}

	// One set-based, LSN-fenced statement. This is where the guarantee lives that
	// a stale part cannot overwrite a row the change stream already updated.
	if _, err := tx.ExecContext(ctx, d.MergeStaging(spec, staging, m.Columns, p.ExtractLSN)); err != nil {
		return fmt.Errorf("merging part %d: %w", p.Index, err)
	}

	if _, err := tx.ExecContext(ctx, d.DropStagingTable(staging)); err != nil {
		return fmt.Errorf("dropping staging table: %w", err)
	}
	for _, stmt := range d.RestoreSessionSettings() {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("restoring session settings: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing part %d: %w", p.Index, err)
	}
	committed = true

	return l.app.Store.MarkPartLoaded(ctx, spec.Source, p.Index, p.Rows, p.SHA256)
}
