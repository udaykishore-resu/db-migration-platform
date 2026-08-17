package crypto

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/udaykishore-resu/db-migration-platform/internal/model"
)

// Provider applies a table's declared column protections to row images.
//
// It is the only component that ever sees plaintext, and it is intended to run
// on the source side of the network boundary: inside the on-premise extractor
// and inside the CDC transform that runs before events are produced to the
// broker. Nothing downstream of it — object storage, the broker, the applier,
// the target database, the dead-letter store — receives a protected column in
// clear form.
type Provider struct {
	tokenizer *Tokenizer
	cipher    *Cipher
	source    KeySource

	// Shape selects the token format for a fully-qualified column, allowing a
	// 16-digit card number to stay 16 digits while an opaque identifier becomes
	// a fixed-width base32 string.
	shapes map[string]Format
	// DefaultShape applies to columns with no explicit entry.
	defaultShape Format
}

// Options configures a Provider.
type Options struct {
	// ColumnFormats maps "schema.table.column" to a token format.
	ColumnFormats map[string]Format
	// DefaultFormat applies where no column-specific format is set.
	DefaultFormat Format
	// KeyTimeout bounds how long key acquisition may take at startup.
	KeyTimeout time.Duration
}

// NewProvider derives the working keys from the key source and builds the
// tokenizer and cipher. Key acquisition happens once, here, rather than per row:
// keeping the HSM or KMS out of the per-row path is what allows the pipeline to
// scale past the key store's operations-per-second ceiling.
func NewProvider(ctx context.Context, src KeySource, opts Options) (*Provider, error) {
	if src == nil {
		return nil, fmt.Errorf("crypto: provider requires a key source")
	}
	timeout := opts.KeyTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	tokenKey, err := src.Key(ctx, PurposeToken)
	if err != nil {
		return nil, fmt.Errorf("crypto: acquiring tokenisation key: %w", err)
	}
	encKey, err := src.Key(ctx, PurposeEncrypt)
	if err != nil {
		return nil, fmt.Errorf("crypto: acquiring encryption key: %w", err)
	}
	c, err := NewCipher(encKey)
	if err != nil {
		return nil, err
	}

	shapes := make(map[string]Format, len(opts.ColumnFormats))
	for k, v := range opts.ColumnFormats {
		shapes[strings.ToLower(k)] = v
	}

	return &Provider{
		tokenizer:    NewTokenizer(tokenKey),
		cipher:       c,
		source:       src,
		shapes:       shapes,
		defaultShape: opts.DefaultFormat,
	}, nil
}

// Describe returns a non-sensitive description of the backing key store, for
// startup logs and for the audit record written at the start of a migration.
func (p *Provider) Describe() string { return p.source.Describe() }

// Domain builds the domain separator for a column. Including the table means the
// same plaintext in two different tables tokenises differently, which prevents
// accidental cross-table correlation through tokenised columns.
func Domain(table model.TableRef, column string) string {
	return strings.ToLower(table.String() + "." + column)
}

// ProtectRow applies the declared protection of every column in the spec to a
// row image, returning a new map. The input map is never mutated, because the
// caller frequently still needs the plaintext key values to build the row key.
//
// Redacted columns are removed entirely rather than nulled, so that a target
// column with a NOT NULL constraint fails loudly at load time instead of
// silently accepting a null where sensitive data was expected.
func (p *Provider) ProtectRow(spec model.TableSpec, row map[string]any) (map[string]any, error) {
	if row == nil {
		return nil, nil
	}
	out := make(map[string]any, len(row))
	for name, value := range row {
		col, ok := spec.Column(name)
		if !ok {
			// Columns absent from the spec are not migrated at all. Dropping
			// them here means an unexpected new source column can never leak
			// into the target unprotected.
			continue
		}
		protected, keep, err := p.protectValue(spec, col, value)
		if err != nil {
			return nil, fmt.Errorf("protecting %s.%s: %w", spec.Source, name, err)
		}
		if keep {
			out[name] = protected
		}
	}
	return out, nil
}

// protectValue applies one column's protection. The boolean result reports
// whether the column should be present in the output at all.
func (p *Provider) protectValue(spec model.TableSpec, col model.ColumnSpec, value any) (protected any, keep bool, err error) {
	switch col.Protect {
	case "", model.ProtectNone:
		return value, true, nil

	case model.ProtectRedact:
		return nil, false, nil

	case model.ProtectTokenize:
		if value == nil {
			// A null stays null: tokenising it would invent a value that was
			// never there and would break NULL-aware queries on the target.
			return nil, true, nil
		}
		s := asString(value)
		domain := Domain(spec.Source, col.Name)
		return p.tokenizer.Token(domain, s, p.formatFor(domain)), true, nil

	case model.ProtectEncrypt:
		if value == nil {
			return nil, true, nil
		}
		s := asString(value)
		ct, err := p.cipher.Encrypt(Domain(spec.Source, col.Name), []byte(s))
		if err != nil {
			return nil, false, err
		}
		return ct, true, nil

	default:
		return nil, false, fmt.Errorf("unknown protection mode %q", col.Protect)
	}
}

// ProtectKey applies protection to the columns of a row key, so that the applier
// looks the row up on the target by its protected key. Only deterministic modes
// are permitted on key columns; TableSpec.Validate enforces that at config load,
// and this function assumes it.
func (p *Provider) ProtectKey(spec model.TableSpec, key model.RowKey) (model.RowKey, error) {
	protected := make(map[string]any, key.Len())
	for _, c := range key.Columns() {
		col, ok := spec.Column(c.Name)
		if !ok {
			protected[c.Name] = c.Value
			continue
		}
		v, keep, err := p.protectValue(spec, col, c.Value)
		if err != nil {
			return model.RowKey{}, err
		}
		if keep {
			protected[c.Name] = v
		}
	}
	return model.NewRowKey(protected), nil
}

// VerifyToken checks a token against a candidate plaintext, which is how an
// operator confirms one specific record migrated correctly without the pipeline
// ever gaining the ability to reverse a token in bulk.
func (p *Provider) VerifyToken(table model.TableRef, column, plaintext, token string) bool {
	domain := Domain(table, column)
	return p.tokenizer.Verify(domain, plaintext, token, p.formatFor(domain))
}

func (p *Provider) formatFor(domain string) Format {
	if f, ok := p.shapes[domain]; ok {
		return f
	}
	return p.defaultShape
}

// EncryptPayload protects an arbitrary blob, used to encrypt dead-lettered
// events at rest. A dead-letter table is a durable copy of production data with
// a longer retention than the pipeline itself, so it gets the same treatment as
// the target.
func (p *Provider) EncryptPayload(domain string, payload []byte) (string, error) {
	return p.cipher.Encrypt(domain, payload)
}

// DecryptPayload reverses EncryptPayload for the repair worker, which must
// replay the original event bytes.
func (p *Provider) DecryptPayload(domain, encoded string) ([]byte, error) {
	return p.cipher.Decrypt(domain, encoded)
}

// Close releases key material.
func (p *Provider) Close() error { return p.source.Close() }

// asString renders a value for protection. Numeric and temporal values are
// normalised through the same canonical form used for row keys, so a column that
// arrives as int64 from the snapshot and float64 from JSON tokenises identically
// — without this, the snapshot and CDC paths would produce different tokens for
// the same row and reconciliation would report drift that does not exist.
func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case nil:
		return ""
	default:
		return model.CanonicalValue(v)
	}
}
