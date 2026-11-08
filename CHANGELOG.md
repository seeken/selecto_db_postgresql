CHANGES
=======

Unreleased
----------

- A root that reads a CTE or VALUES list of its own (a domain over a CTE, its
  domain declaring no `source_schema`) refuses `cte_name_collision` when the
  name is also a relation on the session's search_path: the root read the CTE's
  rows where the host may have meant the table. A query a CASE expression embeds
  is resolved and qualified like a filter subquery: it read the public table
  under a declared schema, or refused `relation_unresolved`. A raw CTE no longer
  raises `BadMapError`.
- Every table a Selecto read names is resolved and qualified: a filter
  subquery's (`{:exists, query}`, `{:subquery, ...}`) and a domain's declared
  non-association joins' tables were not relations of the read, so they refused
  `relation_unresolved` whenever the outer query did not read the same table,
  read the public table when the subquery declared another schema, or read an
  outer VALUES list of that name. A filter subquery or declared join now
  resolves under its own declaration, or its root's (R59). A domain rooted on a
  CTE its own query defines, or a CTE body rooted on a visible one, reads the
  CTE and is no relation to resolve.
- A closure-table join whose id is its category table's name reads its own
  closure table again. Its generated CTE was named like that table and hid it,
  so the table went unresolved and, with another schema declared in the
  statement, read that schema's closure table. Selecto names every generated
  CTE `__selecto_...` by its content, and refuses any statement where a CTE or
  VALUES name is a relation it reads (`cte_name_collision`) or is defined twice
  differently (`cte_name_redefined`), so every relation a read names is
  resolved and qualified. A Selecto CTE named like a table a body before it
  reads, or like the table it reads itself, which PostgreSQL scoped and
  answered, now refuses.
- `mix compile --warnings-as-errors` passes again: a function sat between
  `operator_types/2`'s `@doc` and its definition.
- A relation named by a declared or inherited schema that does not exist there
  refuses `relation_unresolved`, typed, instead of a raw 42P01 from the read.
- Each query of a statement resolves under its own declarations, and a name two
  of them would resolve to two relations refuses with `relation_ambiguous`: a
  subquery's declaration redirected an ad-hoc join of the same name, and union
  members declaring a name differently read one schema. A nested query whose
  domain declares no `source_schema` takes its enclosing query's declared root
  (R59), as an ad-hoc join's table does, and a retargeted query that declares
  none takes its origin's. A CTE or VALUES name hides a table only
  where the SQL can see it, so a name defined in one query no longer hides a
  relation in another, and a CTE's body reads the real relation of a CTE named
  after it later.
- A CTE's or LATERAL join's query resolves, pins and guards like the outer
  query's: on default settings they refused `relation_unresolved`, and under a
  diverging host path a CTE named like a table read without a guard.
- A relation only the temp schema holds names `source_schema: "pg_temp"` in its
  refusal, which a host can declare.
- An ad-hoc join's table and a subquery join's tables resolve, pin and guard like
  the domain's own: on default settings they answer, under a root that declares
  its schema an ad-hoc source resolves there (R59), a subquery's own declared
  schema holds, and a host path that would read another table refuses with
  `relation_ambiguous`.
- Text wholly the host's runs exactly as written, in the host's session (R68, R71):
  no qualification, rewriting or catalog proof, no search_path or settings pin, no
  `temporary_relations` or `relation_ambiguous` guard, and `:sql_unprovable` and the
  statement size bound on it are gone. It must be a read by its first word
  (`:hand_built_not_a_read`; SHOW and EXPLAIN are reads), refuses a non-UTF8 session
  and always runs READ ONLY, so hand-built writes and row locks, on every route,
  refuse `:read_only_read`. Whatever else fails is the driver's error.
- A statement Selecto built that carries a host fragment stays generated: its
  relations, filters and tenant scope keep their pin and guards, only the fragment
  is verbatim, and it runs READ ONLY.
- A read's `:timeout` on a connection the host holds is the server's
  `statement_timeout`: no process is killed, the reply is read, the read is
  undone and the host's transaction goes on.
- A read in a host transaction takes its relation locks before its savepoint, so
  they last to the host's commit, for tables and views alike (R69). Hand-built
  text is judged by its plan, `EXPLAIN (FORMAT JSON)`, under the read's deadline.
- A failed RELEASE or ROLLBACK TO at the end of a read reaches the caller: it is
  retried, then the connection closes. Selecto sends no ROLLBACK or COMMIT for a
  transaction the host owns. A write whose RELEASE SAVEPOINT fails goes back to
  the savepoint, or reports the connection broken (R70).
- `stream/4` of a Selecto read returns its refusals at the call, typed, and stays
  lazy: it holds nothing until enumerated.
- A catalog read in a host transaction runs in a savepoint of its own, so a cancel
  keeps its type and leaves the host's transaction alive.
- Generated SQL: citext against `websearch_to_tsquery` and the like resolves to
  pg_catalog's text `@@`; a column through a `SELECT *` CTE (materialized-path
  joins) keeps its type; `upper(citext)` and two bare literals under NULLIF are
  text, so GREATEST, LEAST, COALESCE and CASE mixing them with citext answer.
- A cancel, a lock timeout or a lost connection during a session or catalog read
  is what the caller is told, never `relation_unresolved` or
  `session_state_unavailable`.

- Builders mark any statement carrying text they did not generate, and its
  fragment stays verbatim. CTE names are read from tokens, SEARCH and CYCLE included,
  and a WITH list that cannot be read refuses, on every generated statement.
  NULLIF's result is coerced to the type its equality took; a volatile first
  operand of NULLIF or a simple CASE refuses `:volatile_operand`. Anything
  rendering past 4 MiB or eight times its size refuses `:statement_too_large`.
  CASE, COALESCE, GREATEST and LEAST take PostgreSQL's common type where it can
  be named.
  client_encoding is read by `pg_client_encoding()`, so UNICODE counts as UTF8.
- Every statement runs unnamed, whatever the configuration (R63): code a
  statement reaches could DEALLOCATE and PREPARE a named one, and the driver
  bound the replacement. The `statement_cache` setting is removed. Transaction
  statements use the simple protocol, marked by the adapter, never chosen from
  a statement's text; a setting left in the configuration logs a warning. A
  read is 7 server statements instead of 14, and a write 17 instead of 22: the
  reset-path lookup is one statement, snapshot and pin one, compare and unpin
  one, with one pg_settings scan per pin and no deferrable count in a read
  (R61). A write tests for any deferrable trigger cheaply and runs the full
  count only when it finds one.
- An Ecto repository's transaction is read from the checked-out connection,
  not Repo.in_transaction?/0, so a read under the SQL Sandbox or after a raw
  BEGIN joins it: Selecto never COMMITs a transaction it did not open.
- A stream after a raw BEGIN refuses, where it used to disconnect the host's
  transaction. An Ecto write's savepoint holds one checkout, under the call's
  timeout. A failed ROLLBACK TO a Selecto savepoint refuses `:connection_broken`.
- A session whose client_encoding is not UTF8 refuses on every read, stream and
  write (R65): parameters are UTF-8 bytes, and a tenant name would be misread
  as another's. The setting is compared with the session's other settings.
- A write on a pid or named connection that finds a transaction it cannot prove
  the caller's refuses `:unowned_transaction`; DBConnection.run/3 joins. NULLIF
  has the type its equality takes, a subscript its element's. Typing a statement
  is a single pass.
- A statement with anything after a semicolon refuses, and a read must be a
  query.
- U& and dollar-quoted strings, and E'' with an escaped quote, are untyped
  literals. A citext pattern operator answers for every pg_catalog string
  (varchar, bpchar, name, SIMILAR TO), as a member for text. LIKE takes ANY/ALL
  over string arrays, and strings concatenated with || are text.
- A function outside pg_catalog is named only as the exact match for its
  arguments' known types or the sole candidate that can take them; an argument
  is never cast to a type it lacks, and anything else is `:function_ambiguous`
  (R62). A function no extension owns, bare or schema-qualified, refuses
  `:function_not_member` unless the domain declares it as a UDF. Whether a word
  before `(` is a function is decided by its position, and `U&"..."` identifiers
  are decoded before qualification and lint. A relation with no schema under a
  root that declares one resolves in the root's schema. An extension's
  composite type (topogeometry) compares through the qualifier, which refuses
  where PostgreSQL has no unique operator. A `read_only_reads` that is no
  boolean refuses at connect.
- A non-member operator on an extension operand refuses in Selecto, except a
  string-category type against a pg_catalog string. A non-member function beside
  an extension's members is ambiguous. Keyword-named functions are qualified.
  Catalog facts key columns by exact name. Reads take the `read_only_reads`
  setting and refuse writes typed (`read_only_read`); row locks are read from
  tokens.

- An operator outside pg_catalog is named only when it is a member of the
  operand type's extension for those operand types; an explicit one is
  verified. Selecto reads run READ ONLY. `explain/2`, `explain_analyze/2` and
  QueryAnalyzer run through the read's qualification, pin and checks;
  `get_query_plan/2` no longer sends TIMING without ANALYZE. The lint exempts
  `name =` only in SET lists. Overloads of one extension resolve in its schema.

- Every generated statement passes through one qualifying emitter
  (`SelectoDBPostgreSQL.Qualify`): operators, by-name constructs and functions
  are written explicitly, and the tagging, hierarchy, OLAP and enhanced-join
  builders run instead of being refused. Extension-schema operators and
  functions cast their operands to the member type; an operand that may be an
  extension type but cannot be resolved refuses (`operand_type_unknown`). Column
  types are verified through the parser outside READ COMMITTED.

- Selecto's catalog SQL (function verification, column catalog, introspection)
  names its operators. `column_catalog/2` resolves its relation as the write
  will: a temp namesake no longer changes the answer, and `pg_temp.<name>`
  declares a temp table.

- uuid and domain-over-uuid columns accept text in guarded writes and scope
  compares; text that is no uuid refuses typed (`invalid_uuid`).

- Bind domain-typed ISO temporal values as Postgrex date/time values in writes
  and compare predicates. Date-shaped text fields remain text, and malformed
  temporal literals fail compilation before dispatch.

- Composite post-image keys use direct row comparisons so PostgreSQL infers
  each bound value from its key column. Empty returned key sets remain empty
  without an untyped NULL subquery; counted writes retain cardinality errors.

- A bound-scope INSERT, UPDATE or UPSERT without a post-image of its own (a
  graph descendant bound by an ancestor) runs the same contract as a
  bound-scope DELETE for its event: ROW EXCLUSIVE lock, statement-level
  triggers and rules across the partition and inheritance tree, deferrable
  triggers, before and after the statement (w3exrev18 item 33a).

- A bound-scope DELETE (graph delete-missing cleanups included) is also
  refused, before and after its statement, while any enabled non-internal
  deferrable trigger exists in the database, as for inserts, updates and
  upserts (w3exrev17 HIGH 2).

- Savepoint names carry a random nonce, so a host savepoint can never share
  one (w3exp5 item 20). README: row-level triggers that modify other rows are
  allowed and are the schema owner's responsibility; statement-level
  triggers, rules and deferrable triggers stay refused, deletes included.

- A bound-scope DELETE (`metadata.bound_scope`, set by SelectoUpdato for an
  engine tenant, trusted or host scope, required filter or query
  enforcement) takes the ROW EXCLUSIVE lock, follows the READ COMMITTED
  rule, and is refused (`query_rule_not_evaluable`) before and again after
  its statement under an enabled statement-level DELETE trigger or DELETE
  rule on the relation or across its partition and inheritance tree, or on
  a view. Before, an AFTER DELETE FOR EACH STATEMENT trigger could rewrite
  another tenant's row (w3exp5 item 17).

- A write on a connection inside the caller's transaction (a checked-out
  Postgrex connection, or an Ecto Repo already in a transaction) runs under
  its own savepoint: a refusal after its DML (a post-image violation) rolls
  that DML back even when the caller ignores the error and commits. An idle
  checked-out connection gets a managed transaction instead of running
  statement by statement (w3exrev16 item 18).

- Every guarded write (metadata `post_image`, `insert_admission`,
  `query_enforcement` or `tenant_fields`) is refused unless the transaction
  is READ COMMITTED (`query_rule_not_evaluable`, `reason:
  :transaction_isolation`), checked inside the write's transaction before
  the DML and again in the post-image re-read. Under REPEATABLE READ or
  SERIALIZABLE catalog reads use the transaction snapshot, so a trigger
  committed after it passed every check and still fired. Transactions the
  adapter opens (Postgrex, or an Ecto Repo not already in a transaction) run
  `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` as their first statement.
  Tests: a host REPEATABLE READ transaction with a concurrently committed
  deferrable trigger, a managed write on a REPEATABLE READ default
  connection, and a row-lock race for the after-DML deferrable check.
- Operational limitation (documented in the README): any enabled deferrable
  trigger anywhere in the database refuses every bound-scope write. This is
  the accepted cross-runtime choice and is not narrowed.

- TW-09 post-image: guarded upserts are re-read like inserts. Refused as
  well (`query_rule_not_evaluable`): any enabled deferrable trigger in the
  database (it fires at commit, after the check), statement triggers and
  rules on any partition of the target, and a target with inheritance
  children.

- Added (TW-09 post-image): a guarded INSERT/UPDATE (`metadata.post_image`)
  returns its rows' keys and is re-read by them after the statement, inside
  the transaction, by one tagged SELECT; unless every written row is still
  inside the scope, the write rolls back with `query_rule_violation`.
  Statement-level and deferrable triggers, rewrite rules, views and keys
  that are not an enforced NOT NULL unique index are refused
  (`query_rule_not_evaluable`). Row triggers (updated_at, audit) keep
  working; triggers that change other rows are outside the guarantee.
  Live tests.

- Fixed: `column_catalog/2` resolves the relation exactly as the write
  quotes it (each dotted part quoted, through the search path, so a
  mixed-case, quoted, schema-qualified or temp-shadowed table reads its own
  columns), and resolves domains (including domains over domains) to their
  base type and typmod through `pg_type.typbasetype`/`typtypmod`. A domain
  chain deeper than 16 levels is reported unresolved with `trust:
  :base_verified`. Live tests.

- Added: `column_catalog/2` reports `format_type(atttypid, atttypmod)` and
  the column collation's `collisdeterministic` for each column of a table
  (search-path resolved; an enum reports base `enum`). SelectoUpdato's
  candidate check uses it as verified metadata (query_enforced_scalar v2).
  Live test.

- Changed (default): the statement cache is now on by default
  (`config :selecto_db_postgresql, statement_cache: true`, 256 hash-slot
  names per connection), so each statement is prepared once per Postgrex
  connection and later executions take one round trip instead of two. Opt
  out with `config :selecto_db_postgresql, statement_cache: false` (or `0`),
  which prepares every statement unnamed as before. Set the opt-out behind a
  transaction-mode pooler: PgBouncer before 1.21, or without
  `max_prepared_statements`, cannot run named statements. Ecto repository
  connections keep the repository's own `:prepare` setting.

- Lock Postgrex to 0.22.4, including the streaming comment and notification
  reconnect security fixes (CVE-2026-66838 and CVE-2026-58225). The existing
  channel-name fix and immutable Core dependency remain in place.

- Render canonical computed-value casts and JSON text extraction in the
  PostgreSQL dialect, preserving SQL nulls, native cast targets and ordered
  parameter markers. The optional callback accepts only the closed fragment
  shape and supports publication before the corresponding Core upgrade.
- Hex assembly uses an explicit metadata-only mode with the documented Core
  version requirement, while standalone source verification keeps its immutable
  Core Git pin.
- Restore the existing Dialyzer gate by using the baseline Core order tuple in
  benchmark queries and making absent-row normalization explicit. Valid query
  results and affected-row counts retain their existing behavior; malformed
  driver row counts fail before they enter portable result metadata.
- The adapter declares `supports?(:execute_timeout)`, so `Selecto.execute/2`
  runs the query in the calling process instead of copying the result out of
  a task. `execute/4` with an integer `:timeout` gives Postgrex the shorter of
  that time and the timeout that applied before (Postgrex's 15 seconds, or an
  Ecto repository's configured `:timeout`) plus a matching `:deadline`, so no
  limit is lengthened. A connection the caller already holds checked out (a
  `%DBConnection{}`, or an Ecto repository inside a transaction or checkout)
  ignores per-call timeouts, so such a statement runs in a separate process
  abandoned at the timeout, as Selecto's task was; so does a pool reference
  owned by another adapter. Without `:timeout`, and on every write path,
  execution is unchanged.

- `execute_write/3` and `execute_prepared_write/3` refuse a write without the
  `Selecto.Write.Authorization` the governed entry point (`SelectoUpdato`)
  issued for exactly that command, batch or graph, with `:ungoverned_write`
  and no row changed. The raw implementations moved to
  `execute_write_unsafe/3` and `execute_prepared_write_unsafe/3`, for trusted
  tooling and adapter tests only.

- Added governed co-domain lookup support with bound prefix search, allowlisted
  text-search configuration, ranking, and live tenant/segment isolation tests.

V 0.5.0
--------

- Removed renderer aliases for the retired `json_extract_path`,
  `json_extract_path_text`, and `json_insert` core operations.
- Raised the Selecto baseline to `0.5.0` and became the sole owner of
  PostgreSQL runtime, Postgrex execution, pooling, transactions, streaming,
  native types/errors, JSONB/array/text-search/interval rendering, hierarchy
  path SQL, query analysis, and benchmark behavior.
- Added the typed PostgreSQL dialect implementation and structured capability
  rejection required by the adapter-neutral core contract.
- Added adapter-owned date/time formatting and extraction, text normalization,
  finite bucket rendering, and case-insensitive comparison SQL used by
  Components' portable query intent.
- Normalized PostgreSQL `numeric`, `decimal`, and `numeric[]` result metadata to
  Selecto's portable decimal types.
- Retained connected function verification, write/graph compilation, live
  protocol tests, and bounded live-differential verification in this package.
- Reuse checked-out `%DBConnection{}` transaction handles directly during
  server-version and write-capability discovery instead of interpreting the
  struct as fresh Postgrex connection options.

V 0.4.12
--------

- Restricted named direct connections to Ecto repositories or registered
  Postgrex connection processes instead of accepting every registered atom.
- Normalized iodata queries before pool cache-key generation, pool execution,
  and cursor streaming.
- Made expanded association and junction-table introspection propagate query
  failures instead of silently returning incomplete metadata.

V 0.4.11
--------

- Added connected registered-function verification using exact `pg_proc`
  catalog identity resolution plus Postgrex parse/describe without argument
  binding or function execution.
- Added fail-closed evidence for missing functions, signature and return-shape
  mismatches, execute privilege, required extensions, minimum PostgreSQL
  version, volatility, unsupported types, and driver/connection failures.
- Added unit coverage and live scalar, predicate, and table-function tests; the
  scalar fixture raises if executed, proving the verification path stops after
  parse/describe.
- Added a separate controlled semantic-fixture suite that executes only after
  connected resolution succeeds and covers null/empty/representative text,
  signed integer boundaries, predicate cases, table row shape, and bounded
  invariants for a volatile result without claiming deterministic semantics.
- Adopted the versioned Selecto write capability protocol and reported the
  PostgreSQL generated-key mechanism explicitly.
- Replaced adapter-local graph materialization and result-selection code with
  the dialect-neutral Selecto 0.4.13 helpers while retaining all PostgreSQL
  SQL, transaction, driver, and MERGE ownership in this package.

V 0.4.10
--------

- Validated portable commands, batches, and graphs before every public preview
  and execution boundary, returning structured errors for malformed values and
  normalized identifier collisions.
- Rejected dead and unregistered direct and pool connection references without
  process exits, propagated stream-producer failures immediately instead of
  timing out, and gave each stream enumeration its own consumer-bound cursor
  delivery identity.
- Bounded cursor delivery to one acknowledgement-gated in-flight chunk and
  drained private protocol messages after cancellation; the version-two stream
  model now proves seven invariants across 120 states and 840 checks, backed by
  fast-producer and live PostgreSQL cancellation conformance tests.
- Centralized test and live-verifier connection parsing, preserving supported
  `sslmode` settings while rejecting malformed credentials, database paths,
  ports, and ambiguous URL options before Postgrex dispatch.
- Validated and quoted materialized-view identifiers before refresh dispatch.
- Added a 48-check bounded adapter-safety model, blocking Credo/Dialyzer/docs
  gates, and live PostgreSQL 13–18 CI coverage.
- Added bounded transaction, stream, and managed-pool event-trace models with
  shortest reproducible counterexample traces.
- Added live Batch/Graph rollback fault injection at every first, middle, and
  last step, plus a 232-case relational differential checker covering nullable
  predicates, joins, aggregates, ordering, limits, and parameter contracts.
- Made stale pool transaction and checked-out-connection paths return
  structured connection errors instead of process exits.
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
