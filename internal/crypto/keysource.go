// Package crypto implements the confidentiality boundary of the platform.
//
// The design rule the whole package exists to enforce: sensitive values are
// protected on the source side of the network boundary, before they are written
// to object storage or produced to a broker, and the target database stores the
// protected form permanently. The migration pipeline never holds a decryption
// key, and the target is never in a position to reveal plaintext.
//
// This is the single largest departure from the common pattern of "encrypt in
// transit, decrypt at the loader, insert plaintext". That pattern protects the
// wire, which TLS already did, and leaves plaintext at rest in the target and in
// the loader's memory — so it buys key custody and nothing else while paying the
// full operational cost of an HSM in the hot path.
package crypto

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// ErrNotReversible is returned when a caller asks to reveal a value that was
// protected with a one-way transform.
var ErrNotReversible = errors.New("crypto: value was protected with a one-way transform and cannot be revealed")

// ErrNoKeyMaterial indicates the configured key source produced no usable key.
var ErrNoKeyMaterial = errors.New("crypto: key source returned no key material")

// KeySource supplies the data encryption key used by ciphers and tokenizers.
//
// Splitting this out is what allows the same pipeline binary to run against a
// SafeNet HSM in production, AWS KMS in a cloud-native deployment, and a static
// test key in CI, without any of the data-path code knowing the difference.
type KeySource interface {
	// Key returns the 32-byte data encryption key for the given purpose. Purpose
	// separation means a compromise of the tokenisation key does not also expose
	// the encryption key.
	Key(ctx context.Context, purpose string) ([]byte, error)
	// Describe returns a non-sensitive description for logs and audit records.
	Describe() string
	// Close releases any session held with the underlying key store.
	Close() error
}

// StaticKeySource holds key material in process memory.
//
// It exists for local development, CI and reproducible tests. It refuses to run
// unless explicitly acknowledged, so it cannot be reached by accident in an
// environment that was meant to use an HSM.
type StaticKeySource struct {
	master       []byte
	acknowledged bool
	mu           sync.Mutex
	derived      map[string][]byte
}

// NewStaticKeySource builds a static source from raw key material. Passing
// acknowledgeInsecure=false makes every Key call fail, which is the desired
// behaviour if configuration accidentally selects this source in production.
func NewStaticKeySource(master []byte, acknowledgeInsecure bool) (*StaticKeySource, error) {
	if len(master) < 32 {
		return nil, fmt.Errorf("%w: static key must be at least 32 bytes, got %d", ErrNoKeyMaterial, len(master))
	}
	return &StaticKeySource{
		master:       append([]byte(nil), master...),
		acknowledged: acknowledgeInsecure,
		derived:      make(map[string][]byte),
	}, nil
}

// NewStaticKeySourceFromEnv reads base64 key material from an environment
// variable. Used by the local docker-compose stack.
func NewStaticKeySourceFromEnv(envVar string, acknowledgeInsecure bool) (*StaticKeySource, error) {
	raw := os.Getenv(envVar)
	if raw == "" {
		return nil, fmt.Errorf("%w: %s is unset", ErrNoKeyMaterial, envVar)
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("crypto: %s is not valid base64: %w", envVar, err)
	}
	return NewStaticKeySource(key, acknowledgeInsecure)
}

// GenerateStaticKey produces random key material, for tests and bootstrapping.
func GenerateStaticKey() ([]byte, error) {
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		return nil, fmt.Errorf("crypto: generating key: %w", err)
	}
	return k, nil
}

// Key derives a purpose-specific key from the master key.
func (s *StaticKeySource) Key(_ context.Context, purpose string) ([]byte, error) {
	if !s.acknowledged {
		return nil, errors.New("crypto: static key source is disabled; set allow_insecure_static_key to use it outside production")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if k, ok := s.derived[purpose]; ok {
		return k, nil
	}
	k := deriveKey(s.master, purpose)
	s.derived[purpose] = k
	return k, nil
}

// Describe returns a non-sensitive description.
func (s *StaticKeySource) Describe() string { return "static(in-process, non-production)" }

// Close zeroes the key material held in memory.
func (s *StaticKeySource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	zero(s.master)
	for _, k := range s.derived {
		zero(k)
	}
	s.derived = map[string][]byte{}
	return nil
}

// Unwrapper unwraps a wrapped data encryption key. AWS KMS, GCP KMS and a
// PKCS#11 HSM all reduce to this one operation, which keeps the key-source
// plumbing identical across deployments.
type Unwrapper interface {
	Unwrap(ctx context.Context, wrapped []byte, context map[string]string) ([]byte, error)
	Describe() string
}

// EnvelopeKeySource holds a wrapped DEK and unwraps it once, on first use.
//
// This is the shape that keeps an HSM out of the hot path. The legacy design
// called the HSM per row, which made HSM operations-per-second the rate limiter
// for the entire migration. Here the HSM is called once per process lifetime to
// unwrap a data key; every row after that is a local AES operation.
type EnvelopeKeySource struct {
	wrapped   []byte
	unwrapper Unwrapper
	encCtx    map[string]string
	ttl       time.Duration

	mu       sync.Mutex
	dek      []byte
	loadedAt time.Time
	derived  map[string][]byte
}

// NewEnvelopeKeySource builds an envelope-backed key source. A zero ttl means
// the unwrapped key is cached for the lifetime of the process; a non-zero ttl
// forces periodic re-unwrapping so that a revoked key stops working promptly.
func NewEnvelopeKeySource(wrapped []byte, u Unwrapper, encryptionContext map[string]string, ttl time.Duration) (*EnvelopeKeySource, error) {
	if len(wrapped) == 0 {
		return nil, fmt.Errorf("%w: wrapped key is empty", ErrNoKeyMaterial)
	}
	if u == nil {
		return nil, errors.New("crypto: envelope key source requires an unwrapper")
	}
	return &EnvelopeKeySource{
		wrapped:   append([]byte(nil), wrapped...),
		unwrapper: u,
		encCtx:    encryptionContext,
		ttl:       ttl,
		derived:   make(map[string][]byte),
	}, nil
}

// Key returns a purpose-specific key derived from the unwrapped DEK.
func (s *EnvelopeKeySource) Key(ctx context.Context, purpose string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	expired := s.ttl > 0 && time.Since(s.loadedAt) > s.ttl
	if s.dek == nil || expired {
		dek, err := s.unwrapper.Unwrap(ctx, s.wrapped, s.encCtx)
		if err != nil {
			return nil, fmt.Errorf("crypto: unwrapping data key: %w", err)
		}
		if len(dek) < 32 {
			return nil, fmt.Errorf("%w: unwrapped key is %d bytes, need at least 32", ErrNoKeyMaterial, len(dek))
		}
		zero(s.dek)
		for k, v := range s.derived {
			zero(v)
			delete(s.derived, k)
		}
		s.dek = dek
		s.loadedAt = time.Now()
	}

	if k, ok := s.derived[purpose]; ok {
		return k, nil
	}
	k := deriveKey(s.dek, purpose)
	s.derived[purpose] = k
	return k, nil
}

// Describe returns a non-sensitive description including the backing store.
func (s *EnvelopeKeySource) Describe() string {
	return "envelope(" + s.unwrapper.Describe() + ")"
}

// Close zeroes cached key material.
func (s *EnvelopeKeySource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	zero(s.dek)
	for _, v := range s.derived {
		zero(v)
	}
	s.dek = nil
	s.derived = map[string][]byte{}
	return nil
}

// zero overwrites a key buffer in place.
func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
