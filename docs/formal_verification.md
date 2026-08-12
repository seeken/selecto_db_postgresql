# PostgreSQL Adapter Formal Verification

The adapter includes a deterministic bounded model for fail-closed boundaries
that do not require a live database.

Run it with:

```sh
SELECTO_ECOSYSTEM_USE_LOCAL=1 mise exec -- mix selecto_db_postgresql.verify
SELECTO_ECOSYSTEM_USE_LOCAL=1 mise exec -- \
  mix selecto_db_postgresql.verify --output tmp/postgresql-verification.json
```

The verifier runs four database-independent finite models:

- `selecto_db_postgresql.adapter_safety.v1` exhaustively crosses four command
  shapes, safe and malicious publication names, and two invalid connection
  shapes. Across 16 states and three invariants, its 48 checks prove that:

  - valid commands preview while malformed commands and normalized identifier
    collisions return structured errors;
  - only safe, quoted materialized-view names reach dispatch;
  - unregistered connection names and unsupported URL options fail before
    Postgrex dispatch.

- `selecto_db_postgresql.transaction_protocol.v1` explores two- and three-step
  Batch and Graph plans. Each step can succeed, fail during execution, or fail
  cardinality at the first, middle, or last position. It proves that durable
  state changes only after every step succeeds and that every bounded failure
  path restores the complete initial state.

- `selecto_db_postgresql.stream_protocol.v2` explores stream construction and
  enumeration in the same or a different process, zero to two chunks, explicit
  queue, delivery, and acknowledgement steps, successful completion,
  transaction and producer errors, producer exits, cancellation, and receive
  timeout. Across 120 reachable states and seven invariants, its 840 checks
  prove enumerator ownership, exact chunk-prefix delivery, at most one
  in-flight chunk, preserved terminal outcomes, producer retirement, and zero
  queued protocol messages at every terminal boundary.

- `selecto_db_postgresql.pool_protocol.v1` explores managed-pool start, reuse,
  execution, pool death, stale-manager retirement, stop, and restart through
  two generations. It proves that successful references target the one live
  registered generation and that dead generations fail closed.

The three protocol models use deterministic breadth-first exploration through
`Selecto.Verification.BoundedTraceModel`; counterexamples include the shortest
event trace that reaches a failed invariant.

## Implementation conformance

The bounded models are abstract proofs, not claims about PostgreSQL or OTP
internals. The test suite separately binds those state machines to the real
implementation:

- PostgreSQL-backed fault injection checks Batch and Graph rollback for driver
  errors and cardinality mismatches at every first/middle/last plan position;
- OTP schedule tests exercise distinct concurrent stream enumerators, a fast
  100-chunk producer bounded by consumer acknowledgement, cancellation with an
  empty protocol mailbox, timeout, transaction errors, and producer retirement;
- managed-pool tests exercise start/reuse/kill/retire/restart/stop generations
  and require stale execute, transaction, and with-connection paths to return
  structured errors rather than exits.

Run the live conformance layer with a supported PostgreSQL URL:

```sh
SELECTO_POSTGRES_TEST_URL=postgres://postgres:postgres@localhost:5432/postgres \
  SELECTO_ECOSYSTEM_USE_LOCAL=1 mise exec -- mix test --include postgres
```

The optional live relational differential lane independently interprets every
subset of two canonical three-row tables, including nullable values, and
compares those results with generated Selecto SQL executed by PostgreSQL. Its
232 cases cover bag projection, null and comparison predicates, parameter
substitution, filter conjunction, inner and left joins, grouping with count and
sum, ordering, and limits:

```sh
SELECTO_POSTGRES_TEST_URL=postgres://postgres:postgres@localhost:5432/postgres \
  SELECTO_ECOSYSTEM_USE_LOCAL=1 mise exec -- mix selecto_db_postgresql.verify_sql
```

That lane reports `proof_level: bounded_live_differential`; it is exhaustive
for the declared tiny tables and query shapes, but depends on a live PostgreSQL
server and is not an unbounded proof of SQL equivalence.

A passing report has `proof_level: bounded_exhaustive`: no counterexample exists
inside each complete finite model. It does not prove arbitrary plan lengths,
arbitrary SQL semantics, PostgreSQL transaction or network internals, BEAM
scheduler fairness, or host callbacks. The live PostgreSQL 13–18 test matrix
covers representative adapter integration boundaries and does not enlarge the
finite proof boundary.
