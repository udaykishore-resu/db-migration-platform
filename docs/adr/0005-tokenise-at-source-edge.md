# ADR 0005 — Tokenise at the source edge; never decrypt in the pipeline

**Status:** Accepted

## Context

The common pattern is encrypt-in-transit, decrypt-at-the-loader, insert
plaintext. It protects the wire — which TLS already did — and leaves plaintext at
rest in the target and in the loader's memory. The target remains fully in scope
for the regulation the encryption was meant to address.

Separately, calling a hardware security module per row makes HSM
operations-per-second the throughput ceiling for the entire migration.

## Decision

Columns marked `tokenize` are replaced with deterministic, format-preserving
tokens before anything leaves the source network. The target stores tokens
permanently and no component on the data path can reverse them.

Key material is obtained through an envelope: the key store is called **once per
process** to unwrap a data key; every row after that is a local AES operation.

## Alternatives considered

**Encrypt in transit, decrypt at the loader.** See Context. Buys key custody and
nothing else.

**AWS KMS External Key Store (XKS).** A genuinely good option where the
requirement is specifically "keys never leave our HSM" — it lets native AWS
tooling work against an on-premise HSM. Complementary rather than alternative:
XKS can back the `Unwrapper` this design already uses.

**Randomised encryption for every sensitive column.** Strongest confidentiality,
but the column can no longer be equality-joined, indexed usefully, or reconciled
without decrypting. Available as `encrypt` for columns only ever read whole.

**True format-preserving encryption (FF3-1).** Reversible and format-preserving,
but reversibility is a liability here: nothing on the data path should be able to
turn a token back into a person. Where reversibility is genuinely required, a
token vault outside the pipeline is the right place for it.

## Consequences

- The target never holds plaintext for tokenised columns, and stays out of scope
  for them.
- Equality joins, indexes and reconciliation continue to work.
- Tokens are one-way. Reversal requires a vault, deliberately out of scope.
- Deterministic tokenisation reveals which rows share a value. That trade is
  right for an identifier and wrong for a low-cardinality attribute, where
  `encrypt` should be used instead.
