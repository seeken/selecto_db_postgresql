# PostgreSQL Adapter Formal Verification

The adapter includes a deterministic bounded model for fail-closed boundaries
that do not require a live database.

Run it with:

```sh
SELECTO_ECOSYSTEM_USE_LOCAL=1 mise exec -- mix selecto_db_postgresql.verify
SELECTO_ECOSYSTEM_USE_LOCAL=1 mise exec -- \
  mix selecto_db_postgresql.verify --output tmp/postgresql-verification.json
```

`selecto_db_postgresql.adapter_safety.v1` exhaustively crosses four command
shapes, safe and malicious publication names, and two invalid connection
shapes. Across 16 states and three invariants, its 48 checks prove that:

- valid commands preview while malformed commands and normalized identifier
  collisions return structured errors;
- only safe, quoted materialized-view names reach dispatch;
- unregistered connection names and unsupported URL options fail before
  Postgrex dispatch.

A passing report has `proof_level: bounded_exhaustive`: no counterexample exists
inside that complete finite model. It does not prove arbitrary SQL semantics,
PostgreSQL transaction behavior, network behavior, or host callbacks. The live
PostgreSQL 13–18 test matrix covers those adapter integration boundaries.
