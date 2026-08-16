package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const minimalPlan = `{
  "name": "db2-to-aurora",
  "tables": [{
    "source": {"schema": "app", "name": "accounts"},
    "target": {"schema": "app", "name": "accounts"},
    "primary_key": ["account_id"],
    "columns": [
      {"name": "account_id", "type": "string"},
      {"name": "ssn", "type": "string", "protect": "tokenize"}
    ]
  }]
}`

func TestLoadAppliesDefaults(t *testing.T) {
	path := write(t, "config.json", `{"migration_id":"mig-1"}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Apply.BatchSize != 1000 || cfg.Extract.LoaderConcurrency != 4 {
		t.Fatalf("defaults not applied: %+v", cfg.Apply)
	}
	if cfg.Cutover.MaxLag != "10s" {
		t.Fatalf("cutover defaults not applied: %s", cfg.Cutover.MaxLag)
	}
}

// A typo in a threshold that silently keeps the default is only discovered
// during the incident it caused.
func TestUnknownFieldsAreRejected(t *testing.T) {
	path := write(t, "config.json", `{"migration_id":"m","cutovr":{"max_lag":"1s"}}`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected a typo'd key to be rejected")
	}
	if !strings.Contains(err.Error(), "cutovr") {
		t.Fatalf("error should name the offending key: %v", err)
	}
}

// The migration ID namespaces offsets, dead letters and chunk state. Sharing one
// between migrations corrupts all three.
func TestMigrationIDIsRequired(t *testing.T) {
	path := write(t, "config.json", `{}`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "migration_id") {
		t.Fatalf("expected a migration_id error, got %v", err)
	}
}

// A misconfiguration must not be able to silently downgrade production to an
// in-process key.
func TestStaticKeySourceIsRefusedInProduction(t *testing.T) {
	path := write(t, "config.json", `{"migration_id":"m","environment":"production","crypto":{"key_source":"static"}}`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "not permitted in production") {
		t.Fatalf("expected production to reject the static key source, got %v", err)
	}

	ok := write(t, "ok.json", `{"migration_id":"m","environment":"production","crypto":{"key_source":"kms","kms_key_id":"arn:aws:kms:us-east-1:1:key/abc"}}`)
	if _, err := Load(ok); err != nil {
		t.Fatalf("kms key source should be accepted in production: %v", err)
	}
}

func TestInvalidDurationsAreCaughtAtStartup(t *testing.T) {
	path := write(t, "config.json", `{"migration_id":"m","cutover":{"max_lag":"ten seconds"}}`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "max_lag") {
		t.Fatalf("expected a duration error naming the field, got %v", err)
	}
}

// Unbounded loading exhausts the target's connection limit; the config must not
// allow expressing it.
func TestLoaderConcurrencyMustBeBounded(t *testing.T) {
	path := write(t, "config.json", `{"migration_id":"m","extract":{"loader_concurrency":0,"max_part_bytes":1}}`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "loader_concurrency") {
		t.Fatalf("expected loader_concurrency to be rejected, got %v", err)
	}
}

func TestPlanIsLoadedAndValidated(t *testing.T) {
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.json")
	if err := os.WriteFile(planPath, []byte(minimalPlan), 0o600); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, []byte(`{"migration_id":"m","plan_file":"`+planPath+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Plan.Tables) != 1 {
		t.Fatalf("plan not loaded: %+v", cfg.Plan)
	}
	if got := cfg.Plan.Tables[0].ProtectedColumns(); len(got) != 1 {
		t.Fatalf("column protection not parsed: %+v", got)
	}
}

// Discovering that a table has no primary key four hours into a migration has
// wasted four hours; the same failure at startup costs nothing.
func TestPlanWithoutPrimaryKeyIsRejectedAtStartup(t *testing.T) {
	bad := `{"name":"p","tables":[{"source":{"name":"t"},"target":{"name":"t"},"primary_key":[],"columns":[{"name":"c","type":"string"}]}]}`
	path := write(t, "plan.json", bad)
	_, err := LoadPlan(path)
	if err == nil || !strings.Contains(err.Error(), "primary key") {
		t.Fatalf("expected a primary key error, got %v", err)
	}
}

// A non-deterministically protected key column cannot be looked up on the
// target, which silently breaks both the apply path and reconciliation.
func TestPlanRejectsNonDeterministicallyProtectedKey(t *testing.T) {
	bad := `{"name":"p","tables":[{"source":{"name":"t"},"target":{"name":"t"},"primary_key":["id"],
	  "columns":[{"name":"id","type":"string","protect":"encrypt"}]}]}`
	path := write(t, "plan.json", bad)
	_, err := LoadPlan(path)
	if err == nil || !strings.Contains(err.Error(), "non-deterministic") {
		t.Fatalf("expected the key protection to be rejected, got %v", err)
	}
}

// Secrets live in the environment so the config file can be committed.
func TestEnvironmentOverridesSecrets(t *testing.T) {
	t.Setenv("TARGET_DSN", "postgres://from-env/db")
	t.Setenv("KAFKA_BROKERS", "b1:9092,b2:9092")

	path := write(t, "config.json", `{"migration_id":"m"}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Target.DSN != "postgres://from-env/db" {
		t.Fatalf("DSN not taken from the environment: %q", cfg.Target.DSN)
	}
	if len(cfg.Kafka.Brokers) != 2 {
		t.Fatalf("brokers not parsed from the environment: %v", cfg.Kafka.Brokers)
	}
}

func TestNamedDSNEnvIsHonoured(t *testing.T) {
	t.Setenv("MY_AURORA_DSN", "postgres://named/db")
	path := write(t, "config.json", `{"migration_id":"m","target":{"engine":"postgres","dsn_env":"MY_AURORA_DSN"}}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Target.DSN != "postgres://named/db" {
		t.Fatalf("named DSN env not read: %q", cfg.Target.DSN)
	}
}

func TestUnsupportedEngineIsRejected(t *testing.T) {
	path := write(t, "config.json", `{"migration_id":"m","target":{"engine":"oracle"}}`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected an unsupported engine to be rejected")
	}
}

func TestTokenFormatParsing(t *testing.T) {
	cfg := Defaults()
	for _, name := range []string{"", "opaque", "preserve_shape", "digits", "DIGITS"} {
		if _, err := cfg.TokenFormat(name); err != nil {
			t.Errorf("%q should parse: %v", name, err)
		}
	}
	if _, err := cfg.TokenFormat("format-preserving-encryption"); err == nil {
		t.Error("expected an unknown token format to be rejected")
	}
}

func TestPolicyAndThresholdConversion(t *testing.T) {
	cfg := Defaults()
	cfg.MigrationID = "m"

	ap := cfg.ApplyPolicy()
	if ap.Base.String() != "100ms" || ap.MaxAttempts != 8 || !ap.Jitter {
		t.Fatalf("apply policy: %+v", ap)
	}
	dp := cfg.DLQPolicy()
	if dp.Base.String() != "30s" || dp.Max.String() != "2h0m0s" {
		t.Fatalf("dlq policy: %+v", dp)
	}
	th := cfg.Thresholds()
	if th.MaxLag.String() != "10s" || !th.RequireReverseReplication {
		t.Fatalf("thresholds: %+v", th)
	}
}

func TestMissingFileIsReportedClearly(t *testing.T) {
	_, err := Load("/nonexistent/config.json")
	if err == nil || !strings.Contains(err.Error(), "reading") {
		t.Fatalf("expected a clear read error, got %v", err)
	}
}
