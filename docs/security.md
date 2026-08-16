# Security and compliance

## Position

Sensitive columns are protected **before they leave the network that owns them**,
and the target stores the protected form permanently. No component on the data
path — object storage, broker, loader, applier, target database, dead-letter
store — can turn a protected value back into a person.

This is a deliberate departure from the common pattern of encrypt-in-transit,
decrypt-at-the-loader, insert-plaintext. That pattern protects the wire, which
TLS already did, and leaves plaintext at rest in the target and in the loader's
memory. It buys key custody — real, but narrower than it appears — while paying
the full operational cost of an HSM and leaving the target fully in scope.

## Protection modes

| Mode | Reversible | Deterministic | Target holds | Use for |
|---|---|---|---|---|
| `none` | — | — | Plaintext | Non-sensitive columns |
| `tokenize` | No (vault only) | Yes | Token | Identifiers, anything joined or indexed |
| `encrypt` | Yes | No | Ciphertext | Free text read whole, never joined |
| `redact` | No | — | Nothing | Data that must not migrate at all |

**`tokenize`** is the default recommendation. The token is a PRF over the
*complete* value, expanded into the required format. Per-character derivation
would be far weaker — an attacker with many tokens could attack one character at a
time — so a one-character change re-randomises the entire token. The domain
(`schema.table.column`) is bound into the PRF, so the same email in two tables
tokenises differently and cannot be correlated across a boundary the data model
never intended.

Its cost is honest and worth stating: deterministic tokenisation reveals which
rows share a value. Right for an account identifier; wrong for a low-cardinality
attribute where equality leakage approaches revealing the value.

**`encrypt`** uses AES-256-GCM with the column domain bound as additional
authenticated data, so a ciphertext lifted from one column fails authentication
rather than decrypting in another. Deterministic mode is available where a column
must remain joinable, using a synthetic nonce derived from the plaintext.

## Key handling

```
KeySource ─┬─ Static      in-process; refuses to run unless explicitly
           │              acknowledged, and rejected outright in production
           │
           └─ Envelope ── Unwrapper ─┬─ AWS KMS
                                     ├─ GCP KMS
                                     ├─ PKCS#11 (SafeNet and equivalents)
                                     └─ KMS External Key Store
```

**The key store is called once per process.** An `Unwrapper` unwraps a data key
at startup; every row after that is a local AES operation. This is not an
optimisation — an HSM does low thousands of operations per second and a migration
needs millions, so a per-row HSM call makes the key store the throughput ceiling
for the entire migration.

Purpose separation: tokenisation, encryption, dead-letter payloads and digests
each use a key derived by HKDF-Expand from the data key under a distinct label, so
compromise of one does not expose the others.

Key material is zeroed on shutdown, and a non-zero `key_ttl` forces periodic
re-unwrapping so a revoked key stops working promptly rather than at the next
restart.

The concrete `Unwrapper` is deliberately not vendored: it needs credentials and
hardware belonging to the deployment. The interface is one method.

## Data at rest and in transit

| Location | Protection |
|---|---|
| `.dat` parts | Protected columns already tokenised or encrypted; object storage encrypted with a customer-managed key |
| Broker | Protected columns already transformed; TLS in transit; encrypted at rest |
| Target tables | Tokens and ciphertext, never plaintext, for protected columns |
| Dead-letter payloads | Encrypted with a purpose-derived key |
| Logs | Row images structurally refused (see below) |

## Logging

The single fastest route to a reportable incident in a migration is a well-meant
debug line that prints a row image. The logger refuses to emit attributes named
`row`, `before`, `after`, `values`, `payload`, `plaintext`, `pii`, `password`,
`secret`, `token`, `credential`, `dsn` or `url` — **regardless of what the calling
code passes**, including through `With()` and inside groups.

Rows are identified in logs, metrics and findings by a truncated SHA-256 of the
canonical key, never by the key itself.

## Network

- Object storage reached through a VPC gateway endpoint; no public path.
- Databases in private subnets, no public accessibility.
- Broker with TLS and, where available, mTLS.
- On-premise to cloud over a private circuit (Direct Connect, Interconnect or
  equivalent), never the public internet.

## Compliance mapping

**PCI-DSS.** Tokenised columns take the target out of scope for those columns.
Card data can be `redact`ed so it never migrates. The audit trail is
`migration_ctl.cutover_audit` and `recon_run`.

**NIST 800-53.** AC-3 via IAM and per-service database roles; AU-2/AU-3 via
structured audit logging; SC-8 via TLS everywhere; SC-13 and SC-28 via AES-256
with HSM- or KMS-held keys; CM-3 via the change-controlled migration plan.

**GDPR.** Tokenisation is pseudonymisation under Article 4(5): the additional
information needed to re-identify is held separately, outside the pipeline.
`redact` supports data minimisation for fields with no lawful basis to migrate.

## Threat model

| Threat | Mitigation | Residual |
|---|---|---|
| Target database compromised | Protected columns hold tokens only | Unprotected columns exposed; classify carefully |
| Broker compromised | Values already transformed; TLS | Message metadata (table, timing, volume) visible |
| Object storage bucket exposed | Values already transformed; CMK encryption | Part sizes and row counts leak coarse volume |
| Pipeline process compromised | Holds a data key but not plaintext of tokenised columns | Can encrypt new values; cannot reverse tokens |
| Insider with database access | Same as target compromise | Detokenisation requires separate vault access |
| Log aggregation compromised | Row images structurally refused; keys hashed | Table names and volumes visible |
| Stale part replayed maliciously | LSN fence rejects it | — |
| Tampered `.dat` part | SHA-256 verified before load | Attacker with write access to both part and manifest |

## Reporting a vulnerability

Do not open a public issue. Contact the maintainer directly.
