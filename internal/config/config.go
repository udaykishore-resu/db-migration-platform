// Package config loads and validates platform configuration.
//
// Configuration is JSON on disk with environment overrides for anything that
// varies per environment or that must not be committed. There is no YAML, and no
// configuration library: a migration platform's dependency tree gets audited, and
// a config file format is not worth a transitive dependency graph.
//
// Validation is deliberately strict and happens once, at startup, before any
// connection is opened. A migration that fails after four hours because a table
// in the plan has no primary key has wasted four hours; the same failure at
// startup costs nothing.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/udaykishore-resu/db-migration-platform/internal/control"
	"github.com/udaykishore-resu/db-migration-platform/internal/crypto"
	"github.com/udaykishore-resu/db-migration-platform/internal/dialect"
	"github.com/udaykishore-resu/db-migration-platform/internal/model"
	"github.com/udaykishore-resu/db-migration-platform/internal/retryx"
)

// Config is the whole platform configuration.
type Config struct {
	MigrationID string `json:"migration_id"`
	Environment string `json:"environment"`

	Source  Database       `json:"source"`
	Target  Database       `json:"target"`
	Kafka   Kafka          `json:"kafka"`
	Crypto  Crypto         `json:"crypto"`
	Storage Storage        `json:"storage"`
	Apply   Apply          `json:"apply"`
	Extract Extract        `json:"extract"`
	Recon   Reconciliation `json:"reconciliation"`
	Cutover Cutover        `json:"cutover"`
	Obs     Observability  `json:"observability"`

	// PlanFile points at the migration plan. Keeping the plan in its own file
	// means it can be reviewed, diffed and approved separately from operational
	// settings — the plan is the part that decides what data moves.
	PlanFile string     `json:"plan_file"`
	Plan     model.Plan `json:"-"`
}

// Database is a connection target.
type Database struct {
	Engine dialect.Name `json:"engine"`
	// DSN is normally supplied through the environment, never committed.
	DSN              string `json:"dsn,omitempty"`
	DSNEnv           string `json:"dsn_env,omitempty"`
	MaxOpenConns     int    `json:"max_open_conns"`
	MaxIdleConns     int    `json:"max_idle_conns"`
	ConnMaxLife      string `json:"conn_max_lifetime"`
	StatementTimeout string `json:"statement_timeout"`
}

// Kafka configures the change stream consumer.
type Kafka struct {
	Brokers     []string `json:"brokers"`
	GroupID     string   `json:"group_id"`
	Topics      []string `json:"topics"`
	TLS         bool     `json:"tls"`
	SASL        string   `json:"sasl_mechanism,omitempty"`
	Username    string   `json:"username,omitempty"`
	PasswordEnv string   `json:"password_env,omitempty"`

	MinBytes int    `json:"min_bytes"`
	MaxBytes int    `json:"max_bytes"`
	MaxWait  string `json:"max_wait"`
	// StartOffset is "earliest" or "latest". It only applies when no committed
	// offset exists; otherwise the stored offset always wins, because the stored
	// offset is the one that was written atomically with the data.
	StartOffset string `json:"start_offset"`
}

// Crypto configures the confidentiality boundary.
type Crypto struct {
	// KeySource is "static", "kms" or "pkcs11".
	KeySource string `json:"key_source"`
	// StaticKeyEnv names the environment variable holding base64 key material.
	StaticKeyEnv string `json:"static_key_env,omitempty"`
	// AllowInsecureStaticKey must be set explicitly for the static source to
	// work, so a misconfiguration cannot silently downgrade production.
	AllowInsecureStaticKey bool `json:"allow_insecure_static_key"`

	// WrappedKeyFile holds the wrapped data key for envelope mode.
	WrappedKeyFile    string            `json:"wrapped_key_file,omitempty"`
	KMSKeyID          string            `json:"kms_key_id,omitempty"`
	KMSRegion         string            `json:"kms_region,omitempty"`
	EncryptionContext map[string]string `json:"encryption_context,omitempty"`
	// PKCS11Module is the path to the HSM library, for a SafeNet or equivalent
	// deployment where the key must never leave the appliance.
	PKCS11Module string `json:"pkcs11_module,omitempty"`
	PKCS11Slot   string `json:"pkcs11_slot,omitempty"`
	PKCS11PinEnv string `json:"pkcs11_pin_env,omitempty"`

	// KeyTTL forces periodic re-unwrapping so a revoked key stops working
	// promptly rather than at the next process restart.
	KeyTTL string `json:"key_ttl,omitempty"`

	// DefaultTokenFormat is "opaque", "preserve_shape" or "digits".
	DefaultTokenFormat string            `json:"default_token_format"`
	ColumnTokenFormats map[string]string `json:"column_token_formats,omitempty"`
}

// Storage configures where extracted parts live.
type Storage struct {
	// LocalDir is where the extractor writes parts before they are staged.
	LocalDir string `json:"local_dir"`
	Bucket   string `json:"bucket"`
	Prefix   string `json:"prefix"`
	Region   string `json:"region"`
}

// Apply configures the CDC applier.
type Apply struct {
	BatchSize           int    `json:"batch_size"`
	BatchTimeout        string `json:"batch_timeout"`
	MaxRowsPerStatement int    `json:"max_rows_per_statement"`
	Workers             int    `json:"workers"`

	RetryBase        string `json:"retry_base"`
	RetryMax         string `json:"retry_max"`
	RetryMaxAttempts int    `json:"retry_max_attempts"`

	DLQRetryBase        string `json:"dlq_retry_base"`
	DLQRetryMax         string `json:"dlq_retry_max"`
	DLQRetryMaxAttempts int    `json:"dlq_retry_max_attempts"`
}

// Extract configures Phase 1.
type Extract struct {
	MaxPartBytes int64  `json:"max_part_bytes"`
	MaxPartRows  int64  `json:"max_part_rows"`
	Compress     bool   `json:"compress"`
	Delimiter    string `json:"delimiter"`
	NullSentinel string `json:"null_sentinel"`
	ChunkRows    int    `json:"chunk_rows"`
	// LoaderConcurrency bounds how many parts load at once.
	//
	// This is the single most important throughput knob, and the one most often
	// set wrong. Unbounded concurrency — the natural result of an S3-event-driven
	// design where every object notification starts its own worker — opens a
	// connection per part, exhausts the target's connection limit, and the
	// resulting failures retry into the same wall. A bounded pool is slower in
	// theory and dramatically faster in practice.
	LoaderConcurrency int `json:"loader_concurrency"`
	// SignalSchema is where the watermark table lives on the source.
	SignalSchema string `json:"signal_schema"`
}

// Reconciliation configures verification.
type Reconciliation struct {
	Enabled     bool   `json:"enabled"`
	Interval    string `json:"interval"`
	LeafRows    int64  `json:"leaf_rows"`
	MaxDepth    int    `json:"max_depth"`
	MaxFindings int    `json:"max_findings"`
	Workers     int    `json:"workers"`
	// AutoRepair re-reads and re-applies rows the reconciler flags. Off by
	// default: an automatic repair loop against a systemic bug will happily
	// rewrite the same wrong data forever while reporting healthy.
	AutoRepair bool `json:"auto_repair"`
}

// Cutover configures the gate.
type Cutover struct {
	MaxLag                    string `json:"max_lag"`
	LagStableFor              string `json:"lag_stable_for"`
	MaxOpenDeadLetters        int64  `json:"max_open_dead_letters"`
	MaxReconcileFindings      int    `json:"max_reconcile_findings"`
	MaxReconcileAge           string `json:"max_reconcile_age"`
	RequireAllPartsLoaded     bool   `json:"require_all_parts_loaded"`
	RequireReverseReplication bool   `json:"require_reverse_replication"`
}

// Observability configures logging and metrics.
type Observability struct {
	LogLevel  string `json:"log_level"`
	AdminAddr string `json:"admin_addr"`
	Instance  string `json:"instance"`
}

// Load reads configuration from a file, applies environment overrides, loads the
// plan, and validates everything.
func Load(path string) (*Config, error) {
	cfg := Defaults()

	if path != "" {
		data, err := os.ReadFile(path) //nolint:gosec // path is an operator-supplied flag
		if err != nil {
			return nil, fmt.Errorf("config: reading %s: %w", path, err)
		}
		dec := json.NewDecoder(strings.NewReader(string(data)))
		// Unknown fields are an error rather than a silent no-op: a typo in a
		// threshold that silently keeps the default is exactly the kind of thing
		// that is only discovered during the incident it caused.
		dec.DisallowUnknownFields()
		if err := dec.Decode(cfg); err != nil {
			return nil, fmt.Errorf("config: parsing %s: %w", path, err)
		}
	}

	cfg.applyEnv()

	if cfg.PlanFile != "" {
		plan, err := LoadPlan(cfg.PlanFile)
		if err != nil {
			return nil, err
		}
		cfg.Plan = *plan
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// LoadPlan reads and validates a migration plan.
func LoadPlan(path string) (*model.Plan, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is an operator-supplied flag
	if err != nil {
		return nil, fmt.Errorf("config: reading plan %s: %w", path, err)
	}
	var plan model.Plan
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&plan); err != nil {
		return nil, fmt.Errorf("config: parsing plan %s: %w", path, err)
	}
	if err := plan.Validate(); err != nil {
		return nil, fmt.Errorf("config: plan %s is invalid: %w", path, err)
	}
	return &plan, nil
}

// Defaults returns a configuration with every optional value set.
func Defaults() *Config {
	return &Config{
		Environment: "development",
		Source:      Database{Engine: dialect.Postgres, MaxOpenConns: 8, MaxIdleConns: 4, ConnMaxLife: "30m"},
		Target:      Database{Engine: dialect.Postgres, MaxOpenConns: 16, MaxIdleConns: 8, ConnMaxLife: "30m"},
		Kafka: Kafka{
			GroupID: "db-migration-applier", MinBytes: 1 << 10, MaxBytes: 10 << 20,
			MaxWait: "500ms", StartOffset: "earliest",
		},
		Crypto:  Crypto{KeySource: "static", DefaultTokenFormat: "opaque", KeyTTL: "1h"},
		Storage: Storage{LocalDir: "./parts"},
		Apply: Apply{
			BatchSize: 1000, BatchTimeout: "250ms", MaxRowsPerStatement: 500, Workers: 4,
			RetryBase: "100ms", RetryMax: "30s", RetryMaxAttempts: 8,
			DLQRetryBase: "30s", DLQRetryMax: "2h", DLQRetryMaxAttempts: 12,
		},
		Extract: Extract{
			MaxPartBytes: 2 << 30, MaxPartRows: 20_000_000, Compress: true,
			Delimiter: ",", NullSentinel: `\N`, ChunkRows: 50_000,
			LoaderConcurrency: 4, SignalSchema: "migration_ctl",
		},
		Recon: Reconciliation{
			Enabled: true, Interval: "5m", LeafRows: 5000, MaxDepth: 48,
			MaxFindings: 1000, Workers: 2, AutoRepair: false,
		},
		Cutover: Cutover{
			MaxLag: "10s", LagStableFor: "15m", MaxOpenDeadLetters: 0,
			MaxReconcileFindings: 0, MaxReconcileAge: "30m",
			RequireAllPartsLoaded: true, RequireReverseReplication: true,
		},
		Obs: Observability{LogLevel: "info", AdminAddr: ":9090"},
	}
}

// applyEnv overlays environment variables. Secrets are only ever read from the
// environment, never from the config file, so the file can be committed.
func (c *Config) applyEnv() {
	if v := os.Getenv("MIGRATION_ID"); v != "" {
		c.MigrationID = v
	}
	if v := os.Getenv("ENVIRONMENT"); v != "" {
		c.Environment = v
	}
	if v := os.Getenv("SOURCE_DSN"); v != "" {
		c.Source.DSN = v
	} else if c.Source.DSNEnv != "" {
		c.Source.DSN = os.Getenv(c.Source.DSNEnv)
	}
	if v := os.Getenv("TARGET_DSN"); v != "" {
		c.Target.DSN = v
	} else if c.Target.DSNEnv != "" {
		c.Target.DSN = os.Getenv(c.Target.DSNEnv)
	}
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		c.Kafka.Brokers = strings.Split(v, ",")
	}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		c.Obs.LogLevel = v
	}
	if v := os.Getenv("ADMIN_ADDR"); v != "" {
		c.Obs.AdminAddr = v
	}
	if v := os.Getenv("INSTANCE"); v != "" {
		c.Obs.Instance = v
	}
	if v := os.Getenv("PLAN_FILE"); v != "" {
		c.PlanFile = v
	}
	if v := os.Getenv("LOADER_CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.Extract.LoaderConcurrency = n
		}
	}
	if c.Obs.Instance == "" {
		host, _ := os.Hostname()
		c.Obs.Instance = host
	}
}

// Validate checks the configuration for anything that would fail later.
func (c *Config) Validate() error {
	if c.MigrationID == "" {
		return fmt.Errorf("config: migration_id is required; it namespaces offsets, dead letters and chunk state, and sharing one between migrations corrupts all three")
	}
	if _, err := dialect.For(c.Target.Engine); err != nil {
		return fmt.Errorf("config: target: %w", err)
	}
	if c.Source.Engine != "" {
		if _, err := dialect.For(c.Source.Engine); err != nil {
			return fmt.Errorf("config: source: %w", err)
		}
	}

	for _, d := range []struct {
		name string
		v    string
	}{
		{"apply.batch_timeout", c.Apply.BatchTimeout},
		{"apply.retry_base", c.Apply.RetryBase},
		{"apply.retry_max", c.Apply.RetryMax},
		{"apply.dlq_retry_base", c.Apply.DLQRetryBase},
		{"apply.dlq_retry_max", c.Apply.DLQRetryMax},
		{"reconciliation.interval", c.Recon.Interval},
		{"cutover.max_lag", c.Cutover.MaxLag},
		{"cutover.lag_stable_for", c.Cutover.LagStableFor},
		{"cutover.max_reconcile_age", c.Cutover.MaxReconcileAge},
	} {
		if d.v == "" {
			continue
		}
		if _, err := time.ParseDuration(d.v); err != nil {
			return fmt.Errorf("config: %s is not a valid duration: %w", d.name, err)
		}
	}

	if c.Apply.BatchSize <= 0 {
		return fmt.Errorf("config: apply.batch_size must be positive")
	}
	if c.Extract.LoaderConcurrency <= 0 {
		return fmt.Errorf("config: extract.loader_concurrency must be positive; unbounded loading exhausts the target's connection limit")
	}
	if c.Extract.MaxPartBytes <= 0 {
		return fmt.Errorf("config: extract.max_part_bytes must be positive")
	}

	switch c.Crypto.KeySource {
	case "static":
		if c.Environment == "production" && !c.Crypto.AllowInsecureStaticKey {
			return fmt.Errorf("config: the static key source is not permitted in production; configure kms or pkcs11")
		}
	case "kms":
		if c.Crypto.KMSKeyID == "" {
			return fmt.Errorf("config: crypto.kms_key_id is required for the kms key source")
		}
	case "pkcs11":
		if c.Crypto.PKCS11Module == "" {
			return fmt.Errorf("config: crypto.pkcs11_module is required for the pkcs11 key source")
		}
	default:
		return fmt.Errorf("config: unknown crypto.key_source %q (want static, kms or pkcs11)", c.Crypto.KeySource)
	}

	if _, err := c.TokenFormat(c.Crypto.DefaultTokenFormat); err != nil {
		return err
	}
	for col, f := range c.Crypto.ColumnTokenFormats {
		if _, err := c.TokenFormat(f); err != nil {
			return fmt.Errorf("config: column %s: %w", col, err)
		}
	}

	if len(c.Plan.Tables) > 0 {
		if err := c.Plan.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// TokenFormat maps a configured name to a crypto format.
func (c *Config) TokenFormat(name string) (crypto.Format, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "opaque":
		return crypto.FormatOpaque, nil
	case "preserve_shape", "preserve-shape":
		return crypto.FormatPreserveShape, nil
	case "digits":
		return crypto.FormatDigits, nil
	default:
		return crypto.FormatOpaque, fmt.Errorf("config: unknown token format %q (want opaque, preserve_shape or digits)", name)
	}
}

// CryptoOptions builds the crypto provider options.
func (c *Config) CryptoOptions() (crypto.Options, error) {
	def, err := c.TokenFormat(c.Crypto.DefaultTokenFormat)
	if err != nil {
		return crypto.Options{}, err
	}
	formats := make(map[string]crypto.Format, len(c.Crypto.ColumnTokenFormats))
	for col, name := range c.Crypto.ColumnTokenFormats {
		f, err := c.TokenFormat(name)
		if err != nil {
			return crypto.Options{}, err
		}
		formats[col] = f
	}
	return crypto.Options{DefaultFormat: def, ColumnFormats: formats}, nil
}

// ApplyPolicy builds the apply-path retry policy.
func (c *Config) ApplyPolicy() retryx.Policy {
	return retryx.Policy{
		Base:        mustDuration(c.Apply.RetryBase, 100*time.Millisecond),
		Max:         mustDuration(c.Apply.RetryMax, 30*time.Second),
		Multiplier:  2,
		MaxAttempts: c.Apply.RetryMaxAttempts,
		Jitter:      true,
	}
}

// DLQPolicy builds the dead-letter drain policy.
func (c *Config) DLQPolicy() retryx.Policy {
	return retryx.Policy{
		Base:        mustDuration(c.Apply.DLQRetryBase, 30*time.Second),
		Max:         mustDuration(c.Apply.DLQRetryMax, 2*time.Hour),
		Multiplier:  3,
		MaxAttempts: c.Apply.DLQRetryMaxAttempts,
		Jitter:      true,
	}
}

// Thresholds builds the cutover gate thresholds.
func (c *Config) Thresholds() control.Thresholds {
	return control.Thresholds{
		MaxLag:                    mustDuration(c.Cutover.MaxLag, 10*time.Second),
		LagStableFor:              mustDuration(c.Cutover.LagStableFor, 15*time.Minute),
		MaxOpenDeadLetters:        c.Cutover.MaxOpenDeadLetters,
		MaxReconcileFindings:      c.Cutover.MaxReconcileFindings,
		MaxReconcileAge:           mustDuration(c.Cutover.MaxReconcileAge, 30*time.Minute),
		RequireAllPartsLoaded:     c.Cutover.RequireAllPartsLoaded,
		RequireReverseReplication: c.Cutover.RequireReverseReplication,
	}
}

// Duration parses a configured duration with a fallback.
func mustDuration(s string, fallback time.Duration) time.Duration {
	if s == "" {
		return fallback
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return fallback
	}
	return d
}

// Duration exposes duration parsing to the services.
func Duration(s string, fallback time.Duration) time.Duration { return mustDuration(s, fallback) }
