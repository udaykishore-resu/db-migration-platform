package crypto

import (
	"encoding/base32"
	"strings"
	"unicode"
)

// Tokenizer produces deterministic, format-preserving, one-way tokens.
//
// Determinism is the property that makes the whole migration verifiable: because
// the same plaintext always yields the same token, the reconciler can compare a
// tokenised column on the source against the same column on the target without
// ever decrypting anything, and the target can still index and equi-join on it.
//
// The construction is a PRF over the complete value, expanded into the required
// output format. Deriving each character independently would be far weaker: an
// attacker seeing many tokens could attack one character at a time. Here a single
// bit of plaintext change re-randomises the entire token.
//
// Tokens are one-way by design. Reversal requires a token vault, which is
// deliberately out of scope for the pipeline: no component on the data path holds
// the ability to turn a token back into a person.
type Tokenizer struct {
	key []byte
}

// NewTokenizer builds a tokenizer from a derived key.
func NewTokenizer(key []byte) *Tokenizer {
	return &Tokenizer{key: append([]byte(nil), key...)}
}

// Format describes how a token should be shaped so that it fits the target
// column's type and length without a schema change.
type Format int

const (
	// FormatOpaque emits a fixed-length base32 string. Use when the target column
	// was widened for migration or is already a text column of adequate size.
	FormatOpaque Format = iota
	// FormatPreserveShape emits a token that mirrors the input's character
	// classes and separators: digits map to digits, letters to letters,
	// punctuation is preserved. A 16-digit PAN tokenises to 16 digits and a
	// "123-45-6789" identifier keeps its dashes, so CHAR(11) columns still fit.
	FormatPreserveShape
	// FormatDigits emits digits only, of the same length as the input.
	FormatDigits
)

var b32 = base32.StdEncoding.WithPadding(base32.NoPadding)

// Token produces a token for a value within a column domain. The domain must
// uniquely identify the column, conventionally "schema.table.column".
func (t *Tokenizer) Token(domain, value string, format Format) string {
	if value == "" {
		return ""
	}
	seed := prf(t.key, domain, []byte(value))

	switch format {
	case FormatPreserveShape:
		return shapeToken(seed, value, true)
	case FormatDigits:
		return shapeToken(seed, value, false)
	default:
		// 20 bytes of PRF output rendered as 32 base32 characters: comfortably
		// collision-resistant for any realistic table size, and fits VARCHAR(32).
		return b32.EncodeToString(seed[:20])
	}
}

// Verify reports whether a token corresponds to a plaintext value. This is how
// an operator confirms a specific record migrated correctly without the pipeline
// ever needing the ability to reverse a token.
func (t *Tokenizer) Verify(domain, value, token string, format Format) bool {
	if value == "" && token == "" {
		return true
	}
	expect := t.Token(domain, value, format)
	// Constant-time comparison is unnecessary here (both sides are attacker-
	// visible ciphertext) but costs nothing and avoids a timing-oracle argument
	// during security review.
	return constantTimeEqualString(expect, token)
}

// shapeToken renders PRF output into a string with the same shape as the input.
// keepClasses=false forces every alphanumeric position to a digit.
func shapeToken(seed []byte, input string, keepClasses bool) string {
	stream := newKeystream(seed)
	var b strings.Builder
	b.Grow(len(input))

	for _, r := range input {
		switch {
		case unicode.IsDigit(r):
			b.WriteByte(byte('0' + stream.below(10)))
		case unicode.IsLetter(r) && keepClasses:
			if unicode.IsUpper(r) {
				b.WriteByte(byte('A' + stream.below(26)))
			} else {
				b.WriteByte(byte('a' + stream.below(26)))
			}
		case unicode.IsLetter(r):
			b.WriteByte(byte('0' + stream.below(10)))
		default:
			// Separators, whitespace and punctuation are structural, not
			// sensitive, and preserving them keeps the value parseable by
			// downstream systems that validate format.
			b.WriteRune(r)
		}
	}
	return b.String()
}

// keystream expands a PRF seed into an arbitrarily long byte stream by chaining
// HKDF-Expand blocks, so that a long input never runs out of randomness.
type keystream struct {
	seed  []byte
	block []byte
	pos   int
	round int
}

func newKeystream(seed []byte) *keystream {
	ks := &keystream{seed: seed}
	ks.refill()
	return ks
}

func (k *keystream) refill() {
	k.round++
	k.block = hkdfExpand(k.seed, []byte{byte(k.round >> 8), byte(k.round)}, 32)
	k.pos = 0
}

func (k *keystream) next() byte {
	if k.pos >= len(k.block) {
		k.refill()
	}
	b := k.block[k.pos]
	k.pos++
	return b
}

// below returns a uniform value in [0,n) using rejection sampling. Taking a
// plain modulo would bias the low digits — small, but it is exactly the kind of
// finding that stalls a cryptographic review, and the fix costs nothing.
func (k *keystream) below(n int) byte {
	if n <= 0 || n > 256 {
		return 0
	}
	limit := 256 - (256 % n)
	for {
		v := int(k.next())
		if v < limit {
			return byte(v % n)
		}
	}
}

func constantTimeEqualString(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := 0; i < len(a); i++ {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}
