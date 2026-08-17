package snapshot

import (
	"compress/gzip"
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/udaykishore-resu/db-migration-platform/internal/model"
)

func testSpec() model.TableSpec {
	return model.TableSpec{
		Source:     model.TableRef{Schema: "app", Name: "accounts"},
		Target:     model.TableRef{Schema: "app", Name: "accounts"},
		PrimaryKey: []string{"id"},
		Columns: []model.ColumnSpec{
			{Name: "id", Type: model.TypeInt},
			{Name: "name", Type: model.TypeString},
			{Name: "balance", Type: model.TypeDecimal},
		},
	}
}

func newWriter(t *testing.T, cfg PartWriterConfig) (w *PartWriter, dir string) {
	t.Helper()
	dir = t.TempDir()
	cfg.Dir = dir
	if cfg.Spec.Source.Name == "" {
		cfg.Spec = testSpec()
	}
	var err error
	w, err = NewPartWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return w, dir
}

func TestPartRollsOnRowLimitWithSuffixedNames(t *testing.T) {
	w, dir := newWriter(t, PartWriterConfig{MaxPartRows: 10, ExtractStartLSN: 900})
	for i := 0; i < 25; i++ {
		if err := w.WriteRow("", []any{int64(i), "name", int64(i * 100)}); err != nil {
			t.Fatal(err)
		}
	}
	m, err := w.Close(1000)
	if err != nil {
		t.Fatal(err)
	}

	if len(m.Parts) != 3 {
		t.Fatalf("expected 3 parts for 25 rows at 10 rows/part, got %d", len(m.Parts))
	}
	if m.TotalRows != 25 {
		t.Fatalf("total rows %d, want 25", m.TotalRows)
	}
	// Zero-padded suffixes keep lexical order equal to load order, so an S3
	// prefix listing needs no client-side sorting.
	for i, p := range m.Parts {
		want := "app.accounts.dat." + pad(i)
		if p.Name != want {
			t.Errorf("part %d named %q, want %q", i, p.Name, want)
		}
		if _, err := os.Stat(filepath.Join(dir, p.Name)); err != nil {
			t.Errorf("part file missing: %v", err)
		}
	}
	if m.Parts[0].Rows != 10 || m.Parts[2].Rows != 5 {
		t.Fatalf("unexpected row distribution: %d, %d, %d", m.Parts[0].Rows, m.Parts[1].Rows, m.Parts[2].Rows)
	}
}

func TestPartRollsOnByteLimit(t *testing.T) {
	w, _ := newWriter(t, PartWriterConfig{MaxPartBytes: 200})
	long := strings.Repeat("x", 40)
	for i := 0; i < 40; i++ {
		if err := w.WriteRow("", []any{int64(i), long, int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	m, err := w.Close(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Parts) < 4 {
		t.Fatalf("expected several parts from a 200-byte limit, got %d", len(m.Parts))
	}
}

// A part is only eligible to load once sealed. This is the mechanism that makes
// pipelining the extract and the load safe: the loader can start immediately and
// still never read a file whose tail is a half-written record.
func TestOnSealFiresPerPartAndOnlyForSealedParts(t *testing.T) {
	var sealed []Part
	w, _ := newWriter(t, PartWriterConfig{
		MaxPartRows: 5,
		OnSeal:      func(p Part) error { sealed = append(sealed, p); return nil },
	})
	for i := 0; i < 12; i++ {
		if err := w.WriteRow("", []any{int64(i), "n", int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// Two full parts should already be sealed and loadable while the third is
	// still open.
	if len(sealed) != 2 {
		t.Fatalf("expected 2 parts sealed mid-extract, got %d", len(sealed))
	}
	for _, p := range sealed {
		if !p.Eligible() {
			t.Fatalf("sealed part %d is not eligible: state=%s digest=%q", p.Index, p.State, p.SHA256)
		}
	}
	if _, err := w.Close(1); err != nil {
		t.Fatal(err)
	}
	if len(sealed) != 3 {
		t.Fatalf("expected the final part to seal on Close, got %d total", len(sealed))
	}
}

// A truncated or rewritten part loads without error and silently omits rows. The
// digest is what turns that into a loud failure before any data reaches the
// target.
func TestVerifyPartDetectsTruncation(t *testing.T) {
	w, dir := newWriter(t, PartWriterConfig{MaxPartRows: 1000})
	for i := 0; i < 100; i++ {
		_ = w.WriteRow("", []any{int64(i), "name", int64(i)})
	}
	m, err := w.Close(1)
	if err != nil {
		t.Fatal(err)
	}
	p := m.Parts[0]
	if err := VerifyPart(dir, p); err != nil {
		t.Fatalf("intact part failed verification: %v", err)
	}

	path := filepath.Join(dir, p.Name)
	data, _ := os.ReadFile(path) //nolint:gosec // test fixture
	if err := os.WriteFile(path, data[:len(data)-20], 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyPart(dir, p); err == nil {
		t.Fatal("truncated part passed verification")
	}
}

func TestVerifyPartDetectsCorruption(t *testing.T) {
	w, dir := newWriter(t, PartWriterConfig{})
	for i := 0; i < 20; i++ {
		_ = w.WriteRow("", []any{int64(i), "name", int64(i)})
	}
	m, _ := w.Close(1)
	p := m.Parts[0]

	path := filepath.Join(dir, p.Name)
	data, _ := os.ReadFile(path) //nolint:gosec // test fixture
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyPart(dir, p); err == nil {
		t.Fatal("corrupted part passed verification")
	}
}

// CSV cannot distinguish an empty string from NULL. Writing both as "" turns
// every NULL into an empty string on the target, which no application notices
// until an IS NULL query silently returns nothing.
func TestNullIsDistinctFromEmptyString(t *testing.T) {
	w, dir := newWriter(t, PartWriterConfig{})
	if err := w.WriteRow("", []any{int64(1), nil, int64(0)}); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow("", []any{int64(2), "", int64(0)}); err != nil {
		t.Fatal(err)
	}
	m, err := w.Close(1)
	if err != nil {
		t.Fatal(err)
	}

	records := readPart(t, dir, m.Parts[0])
	if records[0][1] != DefaultNullSentinel {
		t.Fatalf("NULL not written as sentinel: %q", records[0][1])
	}
	if records[1][1] != "" {
		t.Fatalf("empty string not preserved: %q", records[1][1])
	}
}

// A value that happens to equal the NULL sentinel must be escaped, or a record
// whose text is literally the sentinel becomes a NULL on the target.
func TestLiteralSentinelValueIsEscaped(t *testing.T) {
	w, dir := newWriter(t, PartWriterConfig{})
	_ = w.WriteRow("", []any{int64(1), DefaultNullSentinel, int64(0)})
	m, _ := w.Close(1)

	records := readPart(t, dir, m.Parts[0])
	if records[0][1] == DefaultNullSentinel {
		t.Fatal("literal sentinel value was written unescaped and will load as NULL")
	}
}

// Values containing the delimiter, quotes or newlines must survive the round
// trip; unquoted output here is the classic cause of a load that shifts every
// column one to the right for the remainder of the file.
func TestDelimitersAndNewlinesAreQuoted(t *testing.T) {
	w, dir := newWriter(t, PartWriterConfig{})
	nasty := "Doe, John \"Jack\"\nsecond line"
	_ = w.WriteRow("", []any{int64(1), nasty, int64(0)})
	m, _ := w.Close(1)

	records := readPart(t, dir, m.Parts[0])
	if len(records) != 1 {
		t.Fatalf("embedded newline broke record framing: got %d records", len(records))
	}
	if records[0][1] != nasty {
		t.Fatalf("value did not survive the round trip:\n got %q\nwant %q", records[0][1], nasty)
	}
}

func TestCompressedPartsAreGzipAndNamed(t *testing.T) {
	w, dir := newWriter(t, PartWriterConfig{Compress: true})
	for i := 0; i < 50; i++ {
		_ = w.WriteRow("", []any{int64(i), "name", int64(i)})
	}
	m, err := w.Close(1)
	if err != nil {
		t.Fatal(err)
	}
	p := m.Parts[0]
	if !strings.HasSuffix(p.Name, ".gz") || !p.Compressed {
		t.Fatalf("compressed part not marked: %q compressed=%v", p.Name, p.Compressed)
	}
	// The digest must cover the stored (compressed) bytes so it can be compared
	// against an object store checksum without re-deriving anything.
	if err := VerifyPart(dir, p); err != nil {
		t.Fatalf("digest does not match stored bytes: %v", err)
	}
	if records := readPart(t, dir, p); len(records) != 50 {
		t.Fatalf("expected 50 rows through gzip, got %d", len(records))
	}
}

func TestRowKeyBoundsRecordedPerPart(t *testing.T) {
	w, _ := newWriter(t, PartWriterConfig{MaxPartRows: 10})
	for i := 0; i < 20; i++ {
		k := model.NewRowKey(map[string]any{"id": int64(i)}).Canonical()
		_ = w.WriteRow(k, []any{int64(i), "n", int64(i)})
	}
	m, _ := w.Close(1)
	for _, p := range m.Parts {
		if p.FirstKey == "" || p.LastKey == "" {
			t.Fatalf("part %d has no key bounds", p.Index)
		}
		if p.FirstKey == p.LastKey {
			t.Fatalf("part %d bounds collapsed to one key", p.Index)
		}
	}
}

func TestWriteRowRejectsWrongColumnCount(t *testing.T) {
	w, _ := newWriter(t, PartWriterConfig{})
	if err := w.WriteRow("", []any{int64(1)}); err == nil {
		t.Fatal("expected an error for a row with the wrong number of values")
	}
}

// A manifest marked complete while a part is still writing would silently drop
// rows at load time, so validation must reject it.
func TestManifestValidationRejectsIncompleteParts(t *testing.T) {
	m := &Manifest{
		Version: ManifestVersion, SourceTable: "a.b", TargetTable: "a.b",
		Columns: []string{"id"}, Delimiter: ",", Complete: true,
		Parts: []Part{{Index: 0, State: PartWriting}},
	}
	if err := m.Validate(); err == nil {
		t.Fatal("expected validation failure for a complete manifest with a writing part")
	}
}

func TestManifestRoundTripsThroughDisk(t *testing.T) {
	w, dir := newWriter(t, PartWriterConfig{MaxPartRows: 7, ExtractStartLSN: 4242})
	for i := 0; i < 20; i++ {
		_ = w.WriteRow("", []any{int64(i), "n", int64(i)})
	}
	want, err := w.Close(9999)
	if err != nil {
		t.Fatal(err)
	}

	got, err := ReadManifest(filepath.Join(dir, "app.accounts.manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalRows != want.TotalRows || len(got.Parts) != len(want.Parts) {
		t.Fatalf("manifest did not round trip: %+v vs %+v", got, want)
	}
	if got.ExtractStartLSN != 4242 || got.ExtractEndLSN != 9999 {
		t.Fatalf("extract LSN window lost: %d..%d", got.ExtractStartLSN, got.ExtractEndLSN)
	}
	// Every row loaded from a part carries this LSN, which is what lets the
	// fenced upsert reject a stale part that arrives after a fresher change.
	for _, p := range got.Parts {
		if p.ExtractLSN != 4242 {
			t.Fatalf("part %d lost its extract LSN", p.Index)
		}
	}
}

func TestSealedPartsExcludesWritingParts(t *testing.T) {
	m := &Manifest{Parts: []Part{
		{Index: 0, State: PartSealed, SHA256: "abc"},
		{Index: 1, State: PartWriting},
		{Index: 2, State: PartSealed, SHA256: "def"},
	}}
	got := m.SealedParts()
	if len(got) != 2 || got[0].Index != 0 || got[1].Index != 2 {
		t.Fatalf("unexpected sealed parts: %+v", got)
	}
}

// helpers

func pad(i int) string {
	s := "00000" + itoa(i)
	return s[len(s)-5:]
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

func readPart(t *testing.T, dir string, p Part) [][]string {
	t.Helper()
	f, err := os.Open(filepath.Join(dir, p.Name)) //nolint:gosec // test fixture
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	var r *csv.Reader
	if p.Compressed {
		gz, err := gzip.NewReader(f)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = gz.Close() }()
		r = csv.NewReader(gz)
	} else {
		r = csv.NewReader(f)
	}
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("part is not valid CSV: %v", err)
	}
	return records
}
