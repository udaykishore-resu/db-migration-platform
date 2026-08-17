package crypto

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
)

// Purposes used for key separation. A compromise of one derived key must not
// expose any other, so every distinct use of key material gets its own label.
const (
	// PurposeToken derives the key used for deterministic tokenisation.
	PurposeToken = "column-tokenization/v1"
	// PurposeEncrypt derives the key used for reversible column encryption.
	PurposeEncrypt = "column-encryption/v1"
	// PurposeDLQ derives the key used to encrypt dead-lettered payloads at rest.
	PurposeDLQ = "dead-letter-payload/v1"
	// PurposeChecksum derives the key that salts reconciliation digests, so that
	// a leaked checksum table cannot be brute-forced back to low-entropy values.
	PurposeChecksum = "reconciliation-digest/v1"
)

// deriveKey performs HKDF-Expand (RFC 5869) with SHA-256 to produce a 32-byte
// purpose-specific key from a master key.
//
// Expand alone, without a preceding Extract, is the correct construction here:
// the input is already a uniformly random key from an HSM or KMS, not a
// low-entropy password, so the extract step would add cost without adding
// security.
func deriveKey(master []byte, purpose string) []byte {
	return hkdfExpand(master, []byte(purpose), 32)
}

// hkdfExpand implements HKDF-Expand with SHA-256 for output lengths up to
// 255*32 bytes.
func hkdfExpand(prk, info []byte, length int) []byte {
	h := hmac.New(sha256.New, prk)
	out := make([]byte, 0, length)
	var block []byte
	for counter := byte(1); len(out) < length; counter++ {
		h.Reset()
		h.Write(block)
		h.Write(info)
		h.Write([]byte{counter})
		block = h.Sum(nil)
		out = append(out, block...)
	}
	return out[:length]
}

// prf computes a keyed pseudorandom function over a domain and a value. The
// domain is length-prefixed so that a token generated for one column can never
// collide with a token for another column holding the same plaintext — without
// this, joining two tables on their tokenised email columns would silently work
// across tenants that should be isolated.
func prf(key []byte, domain string, value []byte) []byte {
	h := hmac.New(sha256.New, key)
	// Length-prefix the domain so that a domain/value split cannot be forged by
	// choosing a value that reproduces another domain's concatenation. Written
	// through encoding/binary rather than by hand: the manual shift-and-truncate
	// version silently wraps on a domain longer than 4 GiB, and more immediately,
	// it is the kind of code a reviewer has to check rather than read.
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(domain)))
	h.Write(lenBuf[:])
	h.Write([]byte(domain))
	h.Write(value)
	return h.Sum(nil)
}
