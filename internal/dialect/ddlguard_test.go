package dialect

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// MySQL accepts a function as a column default only when it is wrapped in
// parentheses; CURRENT_TIMESTAMP is the single exception. The unparenthesised
// form is a syntax error on MySQL — but MariaDB accepts both spellings, so a
// local MariaDB will not reproduce it and the first thing to notice is CI.
//
// This guard exists because the mistake was made twice: once in the migration
// and once, after that was fixed, in a test fixture that nobody thought to grep.
// A rule enforced in one file is a rule that gets re-broken in the next one, so
// this scans every .sql and .go file in the repository.
func TestNoUnparenthesisedFunctionDefaultsAnywhere(t *testing.T) {
	root := moduleRoot(t)

	// Only functions that are BOTH illegal as a bare MySQL default AND absent
	// from PostgreSQL, so the check cannot fire on a valid Postgres file.
	//
	// CURRENT_TIMESTAMP and NOW are deliberately absent: MySQL permits both bare
	// (NOW is a synonym for CURRENT_TIMESTAMP), and Postgres uses now() as its
	// ordinary default. Flagging those produced false positives on every line of
	// the PostgreSQL migration.
	bad := regexp.MustCompile(`(?i)DEFAULT\s+(UTC_TIMESTAMP|UTC_DATE|UTC_TIME|UUID|RAND)\s*\(`)

	var findings []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "bin", "node_modules", "_to_delete", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		ext := filepath.Ext(path)
		if ext != ".sql" && ext != ".go" {
			return nil
		}
		// This file necessarily contains the pattern it is looking for.
		if filepath.Base(path) == "ddlguard_test.go" {
			return nil
		}

		data, err := os.ReadFile(path) //nolint:gosec // walking our own repository
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(data), "\n") {
			trimmed := strings.TrimSpace(line)
			// Comments may legitimately quote the wrong form to explain it.
			if strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, "//") ||
				strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "*") {
				continue
			}
			if bad.MatchString(line) {
				rel, _ := filepath.Rel(root, path)
				findings = append(findings, rel+":"+itoa(i+1)+": "+trimmed)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the repository: %v", err)
	}

	if len(findings) > 0 {
		t.Errorf("MySQL requires a function default to be parenthesised — write DEFAULT (UTC_TIMESTAMP(6)).\n"+
			"MariaDB accepts the bare form, so this will pass locally and fail on MySQL:\n  %s",
			strings.Join(findings, "\n  "))
	}
}

// moduleRoot walks up from the test's working directory to the directory
// holding go.mod.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not locate the module root")
		}
		dir = parent
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
