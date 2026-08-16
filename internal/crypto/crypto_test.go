package crypto

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/udaykishore-resu/db-migration-platform/internal/model"
)

func testKey(t *testing.T) []byte {
	t.Helper()
	k, err := GenerateStaticKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func testProvider(t *testing.T, opts Options) *Provider {
	t.Helper()
	src, err := NewStaticKeySource(testKey(t), true)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProvider(context.Background(), src, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// ---------------------------------------------------------------- tokenisation

// Determinism is the property the entire verification strategy rests on: if the
// same plaintext did not always produce the same token, the reconciler could not
// compare protected columns across the two databases without decrypting.
func TestTokenIsDeterministic(t *testing.T) {
	tk := NewTokenizer(testKey(t))
	a := tk.Token("app.accounts.email", "uday@example.com", FormatOpaque)
	b := tk.Token("app.accounts.email", "uday@example.com", FormatOpaque)
	if a != b {
		t.Fatalf("tokens differ across calls: %q vs %q", a, b)
	}
	if a == "" {
		t.Fatal("token must not be empty for non-empty input")
	}
}

// Without domain separation, two tables holding the same email would tokenise to
// the same value, silently enabling a cross-table join that the data model — and
// possibly the tenancy boundary — never intended.
func TestTokenDomainSeparation(t *testing.T) {
	tk := NewTokenizer(testKey(t))
	a := tk.Token("app.accounts.email", "uday@example.com", FormatOpaque)
	b := tk.Token("app.disputes.email", "uday@example.com", FormatOpaque)
	if a == b {
		t.Fatal("same plaintext in different columns must not produce the same token")
	}
}

func TestTokenDiffersPerKey(t *testing.T) {
	a := NewTokenizer(testKey(t)).Token("d", "value", FormatOpaque)
	b := NewTokenizer(testKey(t)).Token("d", "value", FormatOpaque)
	if a == b {
		t.Fatal("different keys must produce different tokens")
	}
}

// Format preservation is what allows the target schema to stay unchanged: a
// CHAR(11) national identifier column must still receive 11 characters with the
// dashes in the same positions.
func TestTokenPreservesShape(t *testing.T) {
	tk := NewTokenizer(testKey(t))
	in := "123-45-6789"
	got := tk.Token("app.consumers.ssn", in, FormatPreserveShape)

	if len(got) != len(in) {
		t.Fatalf("length changed: %q -> %q", in, got)
	}
	if got == in {
		t.Fatal("token must not equal plaintext")
	}
	for i := range in {
		switch {
		case in[i] == '-':
			if got[i] != '-' {
				t.Fatalf("separator at %d not preserved: %q", i, got)
			}
		case in[i] >= '0' && in[i] <= '9':
			if got[i] < '0' || got[i] > '9' {
				t.Fatalf("digit class at %d not preserved: %q", i, got)
			}
		}
	}
}

func TestTokenPreservesLetterCase(t *testing.T) {
	tk := NewTokenizer(testKey(t))
	got := tk.Token("d", "AbC-123", FormatPreserveShape)
	if len(got) != 7 {
		t.Fatalf("length changed: %q", got)
	}
	if got[0] < 'A' || got[0] > 'Z' {
		t.Fatalf("uppercase class not preserved: %q", got)
	}
	if got[1] < 'a' || got[1] > 'z' {
		t.Fatalf("lowercase class not preserved: %q", got)
	}
}

func TestTokenDigitsOnlyFormat(t *testing.T) {
	tk := NewTokenizer(testKey(t))
	got := tk.Token("app.cards.pan", "4111111111111111", FormatDigits)
	if len(got) != 16 {
		t.Fatalf("PAN length must be preserved, got %d", len(got))
	}
	for i := 0; i < len(got); i++ {
		if got[i] < '0' || got[i] > '9' {
			t.Fatalf("non-digit at %d in %q", i, got)
		}
	}
}

// Deriving each character independently would let an attacker attack one
// character at a time. A single-character change must re-randomise the whole
// token, which is what feeding the complete value into the PRF buys.
func TestTokenAvalanche(t *testing.T) {
	tk := NewTokenizer(testKey(t))
	a := tk.Token("d", "1234567890123456", FormatDigits)
	b := tk.Token("d", "1234567890123457", FormatDigits)

	same := 0
	for i := range a {
		if a[i] == b[i] {
			same++
		}
	}
	// With independent per-character derivation, only the last character would
	// change. Expect most positions to differ.
	if same > len(a)/2 {
		t.Fatalf("insufficient avalanche: %d of %d characters unchanged (%q vs %q)", same, len(a), a, b)
	}
}

func TestTokenEmptyStaysEmpty(t *testing.T) {
	tk := NewTokenizer(testKey(t))
	if got := tk.Token("d", "", FormatOpaque); got != "" {
		t.Fatalf("empty input must stay empty, got %q", got)
	}
}

func TestTokenVerify(t *testing.T) {
	tk := NewTokenizer(testKey(t))
	tok := tk.Token("d", "secret-value", FormatOpaque)
	if !tk.Verify("d", "secret-value", tok, FormatOpaque) {
		t.Fatal("verify must accept the correct plaintext")
	}
	if tk.Verify("d", "other-value", tok, FormatOpaque) {
		t.Fatal("verify must reject the wrong plaintext")
	}
	if tk.Verify("other-domain", "secret-value", tok, FormatOpaque) {
		t.Fatal("verify must reject the wrong domain")
	}
}

// Digit output must be uniform; a biased modulo would concentrate tokens on low
// digits and measurably shrink the token space.
func TestTokenDigitDistributionIsUniform(t *testing.T) {
	tk := NewTokenizer(testKey(t))
	counts := make([]int, 10)
	const n = 4000
	for i := 0; i < n; i++ {
		tok := tk.Token("d", strings.Repeat("9", 4)+string(rune('a'+i%26))+itoa(i), FormatDigits)
		for _, c := range tok {
			if c >= '0' && c <= '9' {
				counts[c-'0']++
			}
		}
	}
	total := 0
	for _, c := range counts {
		total += c
	}
	expect := float64(total) / 10
	for d, c := range counts {
		dev := float64(c)/expect - 1
		if dev > 0.15 || dev < -0.15 {
			t.Errorf("digit %d deviates %.1f%% from uniform (%d of %d)", d, dev*100, c, total)
		}
	}
}

// ---------------------------------------------------------------------- cipher

func TestCipherRoundTrip(t *testing.T) {
	c, err := NewCipher(deriveKey(testKey(t), "test"))
	if err != nil {
		t.Fatal(err)
	}
	ct, err := c.Encrypt("app.accounts.notes", []byte("free text notes"))
	if err != nil {
		t.Fatal(err)
	}
	if !IsCiphertext(ct) {
		t.Fatalf("output not recognised as ciphertext: %q", ct)
	}
	if strings.Contains(ct, "free text") {
		t.Fatal("plaintext visible in ciphertext")
	}
	pt, err := c.Decrypt("app.accounts.notes", ct)
	if err != nil {
		t.Fatal(err)
	}
	if string(pt) != "free text notes" {
		t.Fatalf("round trip mismatch: %q", pt)
	}
}

func TestCipherRandomisedModeIsNonDeterministic(t *testing.T) {
	c, _ := NewCipher(deriveKey(testKey(t), "test"))
	a, _ := c.Encrypt("d", []byte("same"))
	b, _ := c.Encrypt("d", []byte("same"))
	if a == b {
		t.Fatal("randomised encryption produced identical ciphertext")
	}
}

func TestCipherDeterministicModeIsStable(t *testing.T) {
	c, _ := NewCipher(deriveKey(testKey(t), "test"))
	a, _ := c.EncryptDeterministic("d", []byte("same"))
	b, _ := c.EncryptDeterministic("d", []byte("same"))
	if a != b {
		t.Fatal("deterministic encryption must be stable")
	}
	pt, err := c.Decrypt("d", a)
	if err != nil || string(pt) != "same" {
		t.Fatalf("deterministic decrypt failed: %v %q", err, pt)
	}
}

// The domain is bound as additional authenticated data, so a ciphertext lifted
// from one column and pasted into another must fail rather than decrypt.
func TestCipherRejectsCrossColumnReuse(t *testing.T) {
	c, _ := NewCipher(deriveKey(testKey(t), "test"))
	ct, _ := c.Encrypt("app.accounts.ssn", []byte("123-45-6789"))
	if _, err := c.Decrypt("app.disputes.ssn", ct); err == nil {
		t.Fatal("ciphertext must not decrypt under a different column domain")
	}
}

func TestCipherDetectsTampering(t *testing.T) {
	c, _ := NewCipher(deriveKey(testKey(t), "test"))
	ct, _ := c.Encrypt("d", []byte("value"))
	tampered := ct[:len(ct)-1] + string(rune(ct[len(ct)-1]^0x01))
	if _, err := c.Decrypt("d", tampered); err == nil {
		t.Fatal("tampered ciphertext must fail authentication")
	}
}

func TestCipherRejectsForeignEnvelope(t *testing.T) {
	c, _ := NewCipher(deriveKey(testKey(t), "test"))
	if _, err := c.Decrypt("d", "not-an-envelope"); err == nil {
		t.Fatal("expected rejection of unrecognised envelope")
	}
}

func TestCipherRequires32ByteKey(t *testing.T) {
	if _, err := NewCipher([]byte("short")); err == nil {
		t.Fatal("expected error for undersized key")
	}
}

// ------------------------------------------------------------------ key source

// A static in-process key must never be reachable by accident in an environment
// that was configured to use an HSM.
func TestStaticKeySourceDisabledUnlessAcknowledged(t *testing.T) {
	src, err := NewStaticKeySource(testKey(t), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.Key(context.Background(), PurposeToken); err == nil {
		t.Fatal("unacknowledged static key source must refuse to hand out keys")
	}
}

func TestPurposeSeparation(t *testing.T) {
	src, _ := NewStaticKeySource(testKey(t), true)
	a, _ := src.Key(context.Background(), PurposeToken)
	b, _ := src.Key(context.Background(), PurposeEncrypt)
	if string(a) == string(b) {
		t.Fatal("keys for different purposes must differ")
	}
}

type countingUnwrapper struct {
	calls int
	key   []byte
	err   error
}

func (u *countingUnwrapper) Unwrap(context.Context, []byte, map[string]string) ([]byte, error) {
	u.calls++
	return u.key, u.err
}
func (u *countingUnwrapper) Describe() string { return "test-unwrapper" }

// Keeping the key store out of the per-row path is the difference between a
// migration bounded by the target's write throughput and one bounded by the
// HSM's operations per second.
func TestEnvelopeKeySourceUnwrapsOnce(t *testing.T) {
	u := &countingUnwrapper{key: testKey(t)}
	src, err := NewEnvelopeKeySource([]byte("wrapped"), u, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		if _, err := src.Key(context.Background(), PurposeToken); err != nil {
			t.Fatal(err)
		}
	}
	if u.calls != 1 {
		t.Fatalf("expected exactly 1 unwrap for 1000 key requests, got %d", u.calls)
	}
}

func TestEnvelopeKeySourceReUnwrapsAfterTTL(t *testing.T) {
	u := &countingUnwrapper{key: testKey(t)}
	src, _ := NewEnvelopeKeySource([]byte("wrapped"), u, nil, time.Millisecond)
	if _, err := src.Key(context.Background(), PurposeToken); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if _, err := src.Key(context.Background(), PurposeToken); err != nil {
		t.Fatal(err)
	}
	if u.calls != 2 {
		t.Fatalf("expected re-unwrap after TTL, got %d calls", u.calls)
	}
}

func TestEnvelopeKeySourcePropagatesUnwrapFailure(t *testing.T) {
	sentinel := errors.New("kms: access denied")
	u := &countingUnwrapper{err: sentinel}
	src, _ := NewEnvelopeKeySource([]byte("wrapped"), u, nil, 0)
	if _, err := src.Key(context.Background(), PurposeToken); !errors.Is(err, sentinel) {
		t.Fatalf("expected the underlying KMS error to surface, got %v", err)
	}
}

// -------------------------------------------------------------------- provider

func specForTest() model.TableSpec {
	return model.TableSpec{
		Source:     model.TableRef{Schema: "app", Name: "consumers"},
		Target:     model.TableRef{Schema: "app", Name: "consumers"},
		PrimaryKey: []string{"consumer_id"},
		Columns: []model.ColumnSpec{
			{Name: "consumer_id", Type: model.TypeString, Protect: model.ProtectTokenize},
			{Name: "ssn", Type: model.TypeString, Protect: model.ProtectTokenize},
			{Name: "notes", Type: model.TypeString, Protect: model.ProtectEncrypt},
			{Name: "card_track_data", Type: model.TypeString, Protect: model.ProtectRedact},
			{Name: "created_at", Type: model.TypeTimestamp, Protect: model.ProtectNone},
		},
	}
}

func TestProtectRowAppliesEachMode(t *testing.T) {
	p := testProvider(t, Options{DefaultFormat: FormatOpaque})
	spec := specForTest()
	row := map[string]any{
		"consumer_id":     "C-1001",
		"ssn":             "123-45-6789",
		"notes":           "called on tuesday",
		"card_track_data": "%B4111111111111111^DOE/JOHN^",
		"created_at":      "2026-08-16T12:00:00Z",
	}

	out, err := p.ProtectRow(spec, row)
	if err != nil {
		t.Fatal(err)
	}

	if out["ssn"] == "123-45-6789" {
		t.Error("tokenised column still holds plaintext")
	}
	if !IsCiphertext(out["notes"].(string)) {
		t.Errorf("encrypted column not enveloped: %v", out["notes"])
	}
	// Redacted columns are dropped entirely so a NOT NULL target column fails
	// loudly instead of silently accepting a null where sensitive data belonged.
	if _, present := out["card_track_data"]; present {
		t.Error("redacted column must be absent from the output")
	}
	if out["created_at"] != "2026-08-16T12:00:00Z" {
		t.Errorf("unprotected column changed: %v", out["created_at"])
	}
	// The input map must not be mutated: the caller still needs plaintext keys.
	if row["ssn"] != "123-45-6789" {
		t.Error("ProtectRow mutated its input")
	}
}

// A source column that appears without being declared must never reach the
// target, or a schema change upstream silently leaks unprotected data.
func TestProtectRowDropsUndeclaredColumns(t *testing.T) {
	p := testProvider(t, Options{})
	out, err := p.ProtectRow(specForTest(), map[string]any{
		"consumer_id":     "C-1",
		"newly_added_pii": "sensitive",
		"another_new_col": 42,
		"created_at":      "2026-01-01",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := out["newly_added_pii"]; ok {
		t.Fatal("undeclared column leaked into the protected row")
	}
	if _, ok := out["another_new_col"]; ok {
		t.Fatal("undeclared column leaked into the protected row")
	}
}

func TestProtectRowKeepsNullsNull(t *testing.T) {
	p := testProvider(t, Options{})
	out, err := p.ProtectRow(specForTest(), map[string]any{"consumer_id": "C-1", "ssn": nil})
	if err != nil {
		t.Fatal(err)
	}
	v, present := out["ssn"]
	if !present {
		t.Fatal("null column should be present")
	}
	if v != nil {
		t.Fatalf("null must stay null, got %v", v)
	}
}

// The snapshot path yields int64 and the JSON CDC path yields float64 for the
// same numeric key. If they tokenised differently, the applier would write two
// rows for one source row and the reconciler would report drift forever.
func TestProtectKeyNormalisesNumericTypes(t *testing.T) {
	p := testProvider(t, Options{})
	spec := model.TableSpec{
		Source:     model.TableRef{Schema: "app", Name: "accounts"},
		Target:     model.TableRef{Schema: "app", Name: "accounts"},
		PrimaryKey: []string{"account_id"},
		Columns: []model.ColumnSpec{
			{Name: "account_id", Type: model.TypeInt, Protect: model.ProtectTokenize},
		},
	}

	fromSnapshot, err := p.ProtectKey(spec, model.NewRowKey(map[string]any{"account_id": int64(9931)}))
	if err != nil {
		t.Fatal(err)
	}
	fromCDC, err := p.ProtectKey(spec, model.NewRowKey(map[string]any{"account_id": float64(9931)}))
	if err != nil {
		t.Fatal(err)
	}
	if !fromSnapshot.Equal(fromCDC) {
		t.Fatalf("int64 and float64 keys tokenised differently:\n  snapshot=%s\n  cdc=%s",
			fromSnapshot.Canonical(), fromCDC.Canonical())
	}
}

func TestVerifyTokenRoundTripThroughProvider(t *testing.T) {
	p := testProvider(t, Options{DefaultFormat: FormatPreserveShape})
	spec := specForTest()
	out, err := p.ProtectRow(spec, map[string]any{"consumer_id": "C-1", "ssn": "123-45-6789"})
	if err != nil {
		t.Fatal(err)
	}
	tok, _ := out["ssn"].(string)
	if !p.VerifyToken(spec.Source, "ssn", "123-45-6789", tok) {
		t.Fatal("operator verification of a migrated record failed")
	}
	if p.VerifyToken(spec.Source, "ssn", "999-99-9999", tok) {
		t.Fatal("verification accepted the wrong plaintext")
	}
}

func TestColumnFormatOverride(t *testing.T) {
	p := testProvider(t, Options{
		DefaultFormat: FormatOpaque,
		ColumnFormats: map[string]Format{"app.consumers.ssn": FormatPreserveShape},
	})
	spec := specForTest()
	out, _ := p.ProtectRow(spec, map[string]any{"consumer_id": "C-1", "ssn": "123-45-6789"})
	if got := out["ssn"].(string); len(got) != len("123-45-6789") {
		t.Fatalf("per-column format override ignored, got %q", got)
	}
}

func TestPayloadEncryptionRoundTrip(t *testing.T) {
	p := testProvider(t, Options{})
	ct, err := p.EncryptPayload("dead-letter", []byte(`{"op":"c"}`))
	if err != nil {
		t.Fatal(err)
	}
	pt, err := p.DecryptPayload("dead-letter", ct)
	if err != nil {
		t.Fatal(err)
	}
	if string(pt) != `{"op":"c"}` {
		t.Fatalf("payload round trip mismatch: %q", pt)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
