CHANGES
=======

V 0.4.10
--------

- Validated portable commands, batches, and graphs before every public preview
  and execution boundary, returning structured errors for malformed values and
  normalized identifier collisions.
- Rejected dead and unregistered direct and pool connection references without
  process exits, propagated stream-producer failures immediately instead of
  timing out, and gave each stream enumeration its own consumer-bound cursor
  delivery identity.
- Parsed `SELECTO_POSTGRES_TEST_URL` into current Postgrex options and added
  environment-based connection harness coverage.
- Validated and quoted materialized-view identifiers before refresh dispatch.
- Added a 48-check bounded adapter-safety model, blocking Credo/Dialyzer/docs
  gates, and live PostgreSQL 13–18 CI coverage.
- Removed stale lockfile entries and updated the coordinated Selecto baseline
  to `0.4.12`.
- Bumped the package version to `0.4.10`.

V 0.4.9
--------

- Added atomic execution and preview for portable `Selecto.Write.Graph` values,
  including topological generated-key propagation and normalized root results.
- Added PostgreSQL 17+ owned-set `MERGE` compilation with typed parameter
  sources, `RETURNING`, `merge_action()`, and delete-missing reconciliation.
- Added an atomic ordered fallback for servers without the complete required
  MERGE semantics; newly inserted identities are preserved during cleanup.
- Added connection-aware MERGE capability reporting and real-database tests for
  insert, sync, delete-missing, cross-owner rejection, and whole-graph rollback.
- Fixed placeholder numbering when SQL expressions such as
  `CURRENT_TIMESTAMP` occur between bound assignments.
- Direct graph preview and execution now validate the complete portable graph
  before compiling SQL or opening a transaction.
- Updated the coordinated Selecto dependency baseline to `0.4.11`.
- Bump package version to `0.4.9`.

V 0.4.8
--------

- Added the reusable Selecto write-adapter conformance suite for insert,
  update, upsert, delete, and atomic batch previews.
- Added a real-database release test proving a cardinality mismatch rolls back
  every tentative row mutation.
- Advertised upsert explicitly in adapter write capabilities.
- Require portable upsert commands to carry a domain-governed update-field
  list, reject unknown assignments, and compile an empty list as
  `DO NOTHING`.
- Prevent conflict updates from implicitly rewriting every inserted column,
  including tenant, foreign-key, immutable, and external-identifier fields.
- Updated the coordinated Selecto dependency baseline to `0.4.10`.
- Bump package version to `0.4.8`.

V 0.4.7
--------

- Added adapter-owned compilation for portable non-empty `IN` predicates and
  system-time values used by governed multi-row actions.
- Wrapped individual write commands in PostgreSQL transactions so exact
  cardinality mismatches roll back instead of committing partial mutations.
- Added optional Ecto Repo connection routing inside the PostgreSQL adapter
  while keeping native Postgrex connections independent of Ecto configuration.
- Added regression coverage for bound target identifiers, portable timestamps,
  and transactional cardinality enforcement.
- Bump package version to `0.4.7`.

V 0.4.6
----------

- Bounded atom conversion for PostgreSQL schema, table, column, primary-key,
  and association identifiers while preserving the adapter's published
  atom-keyed introspection contract.
- Updated the coordinated Selecto dependency baseline to `0.4.9`.
- Bump package version to `0.4.6`.

V 0.4.5
----------

- Start the `:postgrex` application before opening direct adapter connections
  so direct adapter connections work reliably in script and generator contexts
  that have not already started dependency applications.
- Bump package version to `0.4.5`.

V 0.4.4
----------

- Updated packaged `selecto` compatibility and README dependency guidance for
  the coordinated Selecto ecosystem point release.
- Bump package version to `0.4.4`.

V 0.4.3
----------

- Dropped the library dependency on `selecto_components`; PostgreSQL adapter
  ownership now stays focused on the Selecto database-adapter contract without
  pulling UI package test/runtime dependencies.
- Updated README dependency guidance for the coordinated point release.
- Bump package version to `0.4.3`.

V 0.4.2
----------

- Dropped the library dependency on `selecto_updato`; PostgreSQL write-adapter
  ownership now stays inside `selecto_updato`'s generic write path instead of a
  package-local `UpdatoAdapter` module.
- Added `list_relations/2` support so PostgreSQL adapter introspection can
  return tables, views, and materialized views for DB-backed generator flows.
- Added `refresh_materialized_view/3` support, including concurrent refresh SQL
  for materialized-view publication workflows.
- Bump package version to `0.4.2`.

V 0.4.1
--------

- Relaxed the `selecto` dependency to allow releases from `0.4.0` up to `0.5.x`.
- Bump package version to `0.4.1`.

V 0.4.0
--------

- Introduced the standalone PostgreSQL adapter package for the external
  Selecto adapter architecture.
- Added PostgreSQL-owned hooks for execution, pooling, streaming, diagnostics,
  server version detection, and repo fallback behavior.
- Dropped the standalone `selecto_db_adapter` dependency now that
  `Selecto.DB.Adapter` ships with `selecto`.
- Updated installation guidance to depend directly on `selecto` plus
  `selecto_db_postgresql`.
- Bump package version to `0.4.0`.
