# Security policy

## Reporting

Please report vulnerabilities privately to the maintainer rather than opening a
public issue. Include a description, reproduction steps, and the impact you
believe it has.

## Scope

This repository is a reference implementation. Before production use, review at
minimum:

- the `Unwrapper` implementation for your key store (not vendored here);
- the protection mode chosen for each column in your migration plan;
- network placement of every component;
- the retention and archival policy for `migration_ctl.dead_letter`, which holds
  a durable copy of production data.

See [docs/security.md](docs/security.md) for the threat model.
