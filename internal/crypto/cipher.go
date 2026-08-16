package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// Ciphertext envelope layout, version-prefixed so that a key rotation or an
// algorithm change can be rolled out without a flag day: a reader can always
// tell how a given value was produced.
const (
	cipherPrefix       = "enc:v1:"
	deterministicMagic = "det:v1:"
)

// Cipher provides reversible column protection.
type Cipher struct {
	aead cipher.AEAD
	// macKey derives the synthetic nonce for deterministic mode.
	macKey []byte
}

// NewCipher builds an AES-256-GCM cipher from a 32-byte derived key.
func NewCipher(key []byte) (*Cipher, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("crypto: cipher requires a 32-byte key, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: aes: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: gcm: %w", err)
	}
	return &Cipher{aead: aead, macKey: deriveKey(key, "synthetic-nonce/v1")}, nil
}

// Encrypt applies randomised authenticated encryption. Each call on the same
// plaintext yields a different ciphertext, which is the strongest option, but
// the result cannot be equality-joined, indexed usefully, or reconciled without
// decrypting. Use it for columns that are only ever read whole.
//
// The domain is bound as additional authenticated data, so a ciphertext lifted
// from one column and pasted into another fails authentication rather than
// silently decrypting.
func (c *Cipher) Encrypt(domain string, plaintext []byte) (string, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("crypto: nonce: %w", err)
	}
	sealed := c.aead.Seal(nil, nonce, plaintext, []byte(domain))
	return cipherPrefix + base64.RawStdEncoding.EncodeToString(append(nonce, sealed...)), nil
}

// EncryptDeterministic applies authenticated encryption with a synthetic nonce
// derived from the plaintext (an SIV-style construction). The same plaintext
// always yields the same ciphertext, so the column remains equality-joinable and
// reconcilable, at the cost of revealing which rows share a value.
//
// That trade-off is the right one for a primary or foreign key column, and the
// wrong one for a low-cardinality attribute where equality leakage is close to
// revealing the value itself.
func (c *Cipher) EncryptDeterministic(domain string, plaintext []byte) (string, error) {
	nonce := prf(c.macKey, domain, plaintext)[:c.aead.NonceSize()]
	sealed := c.aead.Seal(nil, nonce, plaintext, []byte(domain))
	return deterministicMagic + base64.RawStdEncoding.EncodeToString(append(nonce, sealed...)), nil
}

// Decrypt reverses either mode. It is used by operational tooling and by the
// reverse-replication path, never by the forward migration data plane.
func (c *Cipher) Decrypt(domain, encoded string) ([]byte, error) {
	var body string
	switch {
	case strings.HasPrefix(encoded, cipherPrefix):
		body = strings.TrimPrefix(encoded, cipherPrefix)
	case strings.HasPrefix(encoded, deterministicMagic):
		body = strings.TrimPrefix(encoded, deterministicMagic)
	default:
		return nil, errors.New("crypto: value is not a recognised ciphertext envelope")
	}

	raw, err := base64.RawStdEncoding.DecodeString(body)
	if err != nil {
		return nil, fmt.Errorf("crypto: ciphertext is not valid base64: %w", err)
	}
	ns := c.aead.NonceSize()
	if len(raw) < ns+c.aead.Overhead() {
		return nil, errors.New("crypto: ciphertext is too short to be valid")
	}
	plaintext, err := c.aead.Open(nil, raw[:ns], raw[ns:], []byte(domain))
	if err != nil {
		// Do not wrap with detail: a distinguishable error here is a padding
		// oracle in miniature.
		return nil, errors.New("crypto: authentication failed")
	}
	return plaintext, nil
}

// IsCiphertext reports whether a value carries one of the platform's envelopes.
// The reconciler uses this to refuse to compare values it cannot compare.
func IsCiphertext(s string) bool {
	return strings.HasPrefix(s, cipherPrefix) || strings.HasPrefix(s, deterministicMagic)
}
