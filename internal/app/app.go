// Package app is the shared bootstrap for every service binary.
//
// Five services need the same eleven things wired in the same order —
// configuration, logging, metrics, health, key material, database pools, the
// dialect, the store, the admin server, signal handling and graceful shutdown.
// Doing that once here rather than five times in five main functions is not just
// less code: it means a fix to the shutdown path or the connection pool applies
// everywhere, instead of to whichever binary someone remembered.
package app

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
	_ "github.com/lib/pq"              // PostgreSQL driver

	"github.com/udaykishore-resu/db-migration-platform/internal/config"
	"github.com/udaykishore-resu/db-migration-platform/internal/crypto"
	"github.com/udaykishore-resu/db-migration-platform/internal/dialect"
	"github.com/udaykishore-resu/db-migration-platform/internal/obs"
	"github.com/udaykishore-resu/db-migration-platform/internal/store"
)

// App holds everything a service needs.
type App struct {
	Service string
	Cfg     *config.Config
	Log     *slog.Logger
	Metrics *obs.Registry
	Health  *obs.Health

	Target  *sql.DB
	Source  *sql.DB
	Dialect dialect.Dialect
	Crypto  *crypto.Provider
	Store   *store.Store

	admin  *obs.AdminServer
	closes []func() error
}

// Flags are the command-line options every service accepts.
type Flags struct {
	ConfigPath string
	PlanPath   string
	DryRun     bool
	Once       bool
}

// ParseFlags registers and parses the common flags.
func ParseFlags() Flags {
	var f Flags
	flag.StringVar(&f.ConfigPath, "config", envOr("CONFIG_FILE", "config.json"), "path to the configuration file")
	flag.StringVar(&f.PlanPath, "plan", os.Getenv("PLAN_FILE"), "path to the migration plan (overrides the config)")
	flag.BoolVar(&f.DryRun, "dry-run", false, "validate configuration and connectivity, then exit without writing anything")
	flag.BoolVar(&f.Once, "once", false, "run a single pass instead of looping")
	flag.Parse()
	return f
}

// Bootstrap wires a service.
//
// Order matters here. Configuration and key material are resolved before any
// connection is opened, so that a misconfigured migration fails in under a second
// rather than after establishing pools against production databases.
func Bootstrap(ctx context.Context, service string, f Flags, routes func(*http.ServeMux)) (*App, error) {
	cfg, err := config.Load(f.ConfigPath)
	if err != nil {
		return nil, err
	}
	if f.PlanPath != "" {
		plan, err := config.LoadPlan(f.PlanPath)
		if err != nil {
			return nil, err
		}
		cfg.Plan = *plan
	}

	log := obs.NewLogger(os.Stdout, cfg.Obs.LogLevel, service, cfg.Obs.Instance)
	reg := obs.NewRegistry()
	health := obs.NewHealth()

	a := &App{
		Service: service, Cfg: cfg, Log: log, Metrics: reg, Health: health,
	}

	d, err := dialect.For(cfg.Target.Engine)
	if err != nil {
		return nil, err
	}
	a.Dialect = d

	provider, err := buildCrypto(ctx, cfg)
	if err != nil {
		return nil, err
	}
	a.Crypto = provider
	a.closes = append(a.closes, provider.Close)
	log.InfoContext(ctx, "key material ready", slog.String("key_source", provider.Describe()))

	target, err := openDB(ctx, cfg.Target, d.Driver())
	if err != nil {
		return nil, fmt.Errorf("connecting to the target database: %w", err)
	}
	a.Target = target
	a.closes = append(a.closes, target.Close)
	health.Set("target-db", true, "")

	if cfg.Source.DSN != "" {
		sd, err := dialect.For(cfg.Source.Engine)
		if err != nil {
			return nil, err
		}
		src, err := openDB(ctx, cfg.Source, sd.Driver())
		if err != nil {
			return nil, fmt.Errorf("connecting to the source database: %w", err)
		}
		a.Source = src
		a.closes = append(a.closes, src.Close)
		health.Set("source-db", true, "")
	}

	a.Store = store.New(target, d, cfg.MigrationID, provider)
	a.admin = obs.NewAdminServer(cfg.Obs.AdminAddr, reg, health, log, routes)

	log.InfoContext(ctx, "service bootstrapped",
		slog.String("migration_id", cfg.MigrationID),
		slog.String("environment", cfg.Environment),
		slog.String("target_engine", string(cfg.Target.Engine)),
		slog.Int("tables", len(cfg.Plan.Tables)),
		slog.Bool("dry_run", f.DryRun),
	)
	return a, nil
}

// Run starts the admin server and executes the service's main loop, shutting
// down gracefully on SIGINT or SIGTERM.
//
// Graceful shutdown matters more here than in a typical service: the applier may
// be mid-transaction, and a hard kill leaves the target holding locks that the
// next instance will then wait on. Draining cleanly turns a rolling deploy from
// an incident into a non-event.
func (a *App) Run(ctx context.Context, fn func(context.Context) error) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	errCh := make(chan error, 2)
	go func() { errCh <- a.admin.Start(ctx) }()
	go func() { errCh <- fn(ctx) }()

	var firstErr error
	select {
	case err := <-errCh:
		firstErr = err
		cancel()
	case <-ctx.Done():
		a.Log.Info("shutdown signal received, draining")
	}

	// Give the other goroutine a bounded window to finish.
	select {
	case err := <-errCh:
		if firstErr == nil {
			firstErr = err
		}
	case <-time.After(30 * time.Second):
		a.Log.Warn("drain timed out after 30s; exiting anyway")
	}

	a.Close()
	if firstErr != nil && !errors.Is(firstErr, context.Canceled) {
		return firstErr
	}
	return nil
}

// Close releases every resource in reverse order of acquisition.
func (a *App) Close() {
	for i := len(a.closes) - 1; i >= 0; i-- {
		if err := a.closes[i](); err != nil {
			a.Log.Warn("error during shutdown", slog.String("error", err.Error()))
		}
	}
}

// openDB opens and verifies a connection pool.
func openDB(ctx context.Context, c config.Database, driver string) (*sql.DB, error) {
	if c.DSN == "" {
		return nil, errors.New("no DSN configured; set it through the environment rather than the config file")
	}
	db, err := sql.Open(driver, c.DSN)
	if err != nil {
		return nil, err
	}

	// A bounded pool is not a performance tuning detail here, it is a safety
	// mechanism: an unbounded pool against Aurora exhausts max_connections during
	// a load burst, and the resulting failures look like data errors rather than
	// like the resource exhaustion they are.
	db.SetMaxOpenConns(orDefault(c.MaxOpenConns, 16))
	db.SetMaxIdleConns(orDefault(c.MaxIdleConns, 8))
	db.SetConnMaxLifetime(config.Duration(c.ConnMaxLife, 30*time.Minute))
	// Recycling idle connections keeps a failed-over writer endpoint from being
	// held open indefinitely by a pool that has nothing to do.
	db.SetConnMaxIdleTime(5 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// buildCrypto constructs the key source and provider from configuration.
func buildCrypto(ctx context.Context, cfg *config.Config) (*crypto.Provider, error) {
	opts, err := cfg.CryptoOptions()
	if err != nil {
		return nil, err
	}
	opts.KeyTimeout = config.Duration(cfg.Crypto.KeyTTL, time.Hour)

	switch cfg.Crypto.KeySource {
	case "static":
		envVar := cfg.Crypto.StaticKeyEnv
		if envVar == "" {
			envVar = "MIGRATION_STATIC_KEY"
		}
		src, err := crypto.NewStaticKeySourceFromEnv(envVar, cfg.Crypto.AllowInsecureStaticKey)
		if err != nil {
			return nil, err
		}
		return crypto.NewProvider(ctx, src, opts)

	case "kms", "pkcs11":
		// Both reduce to the same envelope shape: unwrap a data key once, then do
		// local AES for every row. Wiring the concrete unwrapper is deployment
		// specific — an AWS KMS client, or a PKCS#11 session against the HSM —
		// and is intentionally not vendored into this repository, because both
		// require credentials and hardware that belong to the deployment rather
		// than to the platform.
		return nil, fmt.Errorf(
			"crypto: the %q key source requires an Unwrapper implementation for your key store; "+
				"see docs/security.md for the interface and the wiring example", cfg.Crypto.KeySource)

	default:
		return nil, fmt.Errorf("crypto: unknown key source %q", cfg.Crypto.KeySource)
	}
}

func orDefault(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
