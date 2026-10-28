# SelectoDBPostgreSQL

PostgreSQL adapter package for the Selecto ecosystem.

This package provides `SelectoDBPostgreSQL.Adapter`, an external adapter module
for using Selecto against PostgreSQL via `postgrex`.

## Installation

```elixir
def deps do
  [
    {:selecto, ">= 0.5.0 and < 0.6.0"},
    {:selecto_db_postgresql, ">= 0.5.0 and < 0.6.0"}
  ]
end
```

## Verification

Run the package tests and the deterministic bounded adapter-safety model with:

```sh
SELECTO_ECOSYSTEM_USE_LOCAL=1 mise exec -- mix precommit
SELECTO_ECOSYSTEM_USE_LOCAL=1 mise exec -- mix selecto_db_postgresql.verify
SELECTO_POSTGRES_TEST_URL=postgres://postgres:postgres@localhost:5432/postgres \
  SELECTO_ECOSYSTEM_USE_LOCAL=1 mise exec -- mix selecto_db_postgresql.verify_sql
```

The database-independent bounded reports and live relational differential report
are complementary to the PostgreSQL matrix; their exact state spaces and
guarantees are documented in
[`docs/formal_verification.md`](docs/formal_verification.md).

Standalone source verification retains the immutable Core Git reference in
`mix.exs` and `mix.lock`. Assemble Hex package metadata separately with:

```sh
SELECTO_ECOSYSTEM_USE_LOCAL=0 SELECTO_HEX_PACKAGE_BUILD=1 mix hex.build
```

This assembly-only mode records the documented Core Hex version requirement,
since Hex packages cannot depend on Git sources. It does not change the source
verification pin or publish either package to a registry.

## Usage

Pass the adapter explicitly when configuring Selecto:

```elixir
selecto =
  Selecto.configure(domain, pg_opts,
    adapter: SelectoDBPostgreSQL.Adapter
  )
```

## Prepared statements (on by default)

The adapter prepares each statement once per Postgrex connection under a
name, so later executions take one round trip (Bind/Execute) instead of two
(Parse/Describe, then Bind/Execute), as Ecto's default `prepare: :named`
does. A name is a hash slot of the SQL (256 slots by default), so each
connection holds at most that many prepared statements however many query
shapes are built. Set another slot count, or turn it off so Postgrex
prepares every statement unnamed:

```elixir
config :selecto_db_postgresql, statement_cache: 1024
config :selecto_db_postgresql, statement_cache: false   # or 0
```

Behind a transaction-mode connection pooler (PgBouncer before 1.21, or 1.21+
without `max_prepared_statements`) named statements do not work, since they
need a server session per connection: set `statement_cache: false`. Ecto
repository connections keep the repository's own `:prepare` setting, and
introspection and function verification stay unnamed.

## Connected database-function verification

The adapter advertises Selecto's `:function_verification` capability. Given a
normalized registered-function signature, `Selecto.verify_function/4` can ask
the connected PostgreSQL database to verify that exact signature before the
application relies on it:

```elixir
{:ok, report} =
  Selecto.verify_function(
    selecto,
    "similarity",
    ["product_name", {:param, "mountain"}],
    call_site: :select,
    mode: :strict
  )

report.status
#=> :database_resolved
```

The PostgreSQL verifier performs two complementary, non-executing checks:

1. It resolves the explicit PostgreSQL identity with `to_regprocedure` and
   reads `pg_proc`/`pg_namespace` metadata for the return shape, set-returning
   flag, volatility, current-database execute privilege, server version, and
   required extensions.
2. It submits a typed `SELECT` to Postgrex's parse/describe operation, then
   immediately closes the unnamed prepared statement. It does not bind values
   or execute the statement.

The verification request contains declared argument types, never runtime
argument values. Its evidence records `function_executed: false` and
`argument_values_transmitted: false`. A successful report proves only that the
exact declared signature resolves for the current connection context and that
the declared result shape and requirements match the current catalog. It does
not prove function semantics for any input.

Selecto types are mapped explicitly to PostgreSQL identities. Notable defaults
are `:string` to `text`, `:decimal` to `numeric`, `:float` to
`double precision`, `:naive_datetime` to `timestamp without time zone`, and
`:utc_datetime` to `timestamp with time zone`; arrays preserve the mapped
element type. `:unknown` and unsupported types produce `:indeterminate`
evidence without database dispatch.

The adapter distinguishes missing names, same-name signature mismatches,
return-shape mismatches, missing `EXECUTE` privilege, unmet extension/version/
volatility requirements, and indeterminate driver or connection failures.
Only `:database_resolved` satisfies Selecto's `mode: :strict` policy.

Connected resolution remains separate from semantic fixture evidence. The live
test suite first requires `:database_resolved`, then executes only package-owned
synthetic functions over null, empty, representative text, integer boundary,
predicate, and table-shape cases. Its volatile fixture asserts only result type,
finite row shape, and range invariants—never a deterministic value. Run these
controlled fixtures explicitly with:

```sh
SELECTO_ECOSYSTEM_USE_LOCAL=1 mise exec -- mix test \
  test/selecto_db_postgresql/function_semantics_integration_test.exs \
  --include postgres
```

Passing those fixtures is `:controlled_live_fixture` evidence for the enumerated
synthetic cases. It is not proof about arbitrary functions or inputs.

## Computed-value compatibility

The optional dialect callback `render_computed_value/2` renders the canonical
cast types and bound JSON text paths delegated by newer Core. It preserves
PostgreSQL cast targets, parameter ordering and SQL null behavior and rejects
unknown operations, targets and malformed fragment shapes.

Install the adapter implementation containing this callback before upgrading
Core to the adapter-owned computed-value compiler. Older Core pins continue to
compile with this adapter and keep their existing behavior. Older adapter commits
without the callback remain usable for ordinary queries, but newer Core refuses
computed casts and JSON text extraction through them. No additional adapter or
portable certification profile is claimed.

## Notes

- Placeholder style is `$N`.
- Identifier quoting uses double quotes.
- Pool-backed execution delegates to `Selecto.ConnectionPool`.
- The adapter declares `supports?(:execute_timeout)`: `Selecto.execute/2` runs
  the query in the calling process, and Postgrex gets the shorter of
  Selecto's remaining `:timeout` and the timeout that applied before
  (Postgrex's 15 seconds, or an Ecto repository's configured `:timeout`).
  Statements on a connection the caller already holds checked out run in a
  separate process abandoned at the timeout. See `execute/4`.

## Governed writes

Applications write through `SelectoUpdato`, which validates every command,
batch, and graph against the domain's `writes` contract and hands this adapter
a single-use `Selecto.Write.Authorization` for exactly that payload.
`execute_write/3` and `execute_prepared_write/3` refuse a write without one
with `:ungoverned_write` before any statement runs, and leave every row
unchanged. `execute_write_unsafe/3` and `execute_prepared_write_unsafe/3` skip
that check; they exist for trusted tooling and this package's own tests, never
for application code.

### Post-image check and its operational limits

A write bound to a tenant or trusted scope (`metadata.post_image`, set by
`SelectoUpdato`) locks its table ROW EXCLUSIVE and resolves it to its oid
before the statement, and is re-read by key after the statement, inside the
same transaction, from the schema-qualified table that oid named and only
among that table's (and its partitions') oids, never through a search_path
lookup a row trigger could redirect. It is rolled back unless every written
row is still inside the scope. The keys are bound back in Postgrex's binary
form, independent of DateStyle, extra_float_digits or any other text-image
setting. The check refuses (`query_rule_not_evaluable`) whatever it cannot
see through:

- **Bound-scope writes refuse while a deferrable trigger that can run user
  code exists anywhere in the database** (the same rule as every other write;
  see "Deferrable triggers" below).
- Statement-level triggers and rewrite rules for the event on the table or
  any of its partitions, views, tables with inheritance (non-partition)
  children, and keys that are not an immediate, valid, non-partial unique
  index over NOT NULL columns whose index collation is each column's own,
  deterministic, with the default operator class (a unique index under
  `"C"` over a case-insensitive column enforces uniqueness by bytes while
  `=` follows the column's collation, so a decoy row could answer).
- A bound-scope DELETE (`metadata.bound_scope`, graph delete-missing
  cleanups included) locks its table ROW EXCLUSIVE and is refused, before
  and again after its statement, under an enabled statement-level DELETE
  trigger or a DELETE rule on the table or anywhere in its partition or
  inheritance tree, while a deferrable trigger that can run user code
  exists, and on a view. A
  graph descendant bound by its root or an ancestor (an insert, update or
  delete on a table without a tenant field) follows the same rule for its
  event.
- **Every guarded write (bound tenant, trusted or host scope, admission
  guard or post-image) requires READ COMMITTED**, checked inside the write's
  transaction (`query_rule_not_evaluable`, `reason: :transaction_isolation`).
  Under REPEATABLE READ or SERIALIZABLE the catalog checks read the
  transaction snapshot, so a trigger committed after it would pass them and
  still fire. Transactions the adapter opens itself (Postgrex or an Ecto
  Repo) run `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` first, whatever
  the connection's `default_transaction_isolation`; a host transaction must
  be READ COMMITTED itself.

**Row-level triggers that modify other rows are allowed, and their effects
on those rows are the schema owner's responsibility.** The check proves the
rows the statement itself wrote; a row-level trigger that updates or deletes
*other* rows, another tenant's included, runs and is not checked.
Statement-level triggers and rewrite rules stay refused under a bound scope,
for inserts, updates, upserts and deletes.

### Session state

A row trigger, or a function a domain view calls, can change the session for
the statements after it: `set_config('search_path' | 'TimeZone' | ...,
..., true)`, or a `TEMP` view, table or type, which PostgreSQL resolves before
the search path for the rest of the session (a temp relation survives
COMMIT). Every write and every Selecto read therefore runs between two
pg_catalog-qualified session snapshots (search_path, TimeZone, DateStyle,
IntervalStyle, extra_float_digits, the oid, xmin and name of every
relation, rewrite rule and type in the session's temp schema, and the
deferrable triggers that can run user code), compared in the client:

- **Writes, bound or not, in any mode.** The snapshot after a write
  statement is the FIRST statement after it, before the re-read or any
  other SQL; it must equal the snapshot the write took first. A change rolls
  the write's transaction, or its savepoint inside the host's transaction,
  back (which also reverts `set_config`) and refuses with
  `query_rule_violation`, `reason: :session_state_changed`.
- **Reads.** A read runs under its own savepoint (a random name) inside the
  host's transaction, or inside its own transaction in autocommit; a change
  rolls it back (which reverts local and session-level `set_config` and any
  temp relation it created) and refuses. A stream checks after every fetched
  batch, before handing its rows out, and rolls its own transaction back on
  a change.
- **Temp shadows.** A statement whose names a temp relation would capture (a
  temp relation named like a relation the statement reads, after FROM, JOIN,
  INTO, UPDATE or TABLE, while a same-named relation exists in a schema of
  the search path; a temp table named like a column refuses nothing), or a
  temp type
  named like one of its identifiers or a type the generated SQL casts to, is
  refused (`query_rule_not_evaluable`, `reason: :temporary_shadow`), for
  reads, streams and writes. A domain over a temp table that shadows nothing,
  and unrelated temp tables, keep working.

**Behaviour change.** A row trigger that runs `CREATE TEMP TABLE IF NOT
EXISTS ...` (a scratch table) changes the session's temp namespace on the
write that creates it. That write is refused and rolled back, which drops
the table again, so every write on that table is refused. Create such
tables up front in the session (or in a regular schema) instead.

Generated write SQL qualifies its casts with `pg_catalog`, and Selecto keeps
no session marker or GUC of its own that a trigger could forge. Beyond
search_path and temp shadowing, a row trigger's other side effects (other
settings, role changes, writes to unrelated tables, DDL) are host-owned
schema behaviour and the schema owner's responsibility.

### Deferrable triggers

A deferrable trigger fires at COMMIT, after every check a write makes, so
**no PostgreSQL write runs while a deferrable trigger that can run user code
exists anywhere in the database**, in a transaction the adapter owns or the
host's (`query_rule_not_evaluable`, `reason: :deferred_triggers`). It is
checked before the write's first DML and again in the first statement after
each DML (a trigger created while the write waited on a lock is seen).
`SET CONSTRAINTS` is never used. Only two kinds of deferrable trigger are
exempt, because PostgreSQL's own code runs them:

- foreign-key triggers whose function is a pg_catalog `RI_FKey_*` function,
  whose equality operators are pg_catalog's, whose referenced unique index
  passes the index test below, and whose key columns on both sides have
  pg_catalog base types (or domains over them): ordinary deferred foreign
  keys keep working;
- unique and exclusion rechecks (`unique_key_recheck`) on an index that
  passes the index test, with pg_catalog exclusion operators.

The index test: no expressions and no predicate; every collation is 0 or
pg_catalog's; every operator class, its family's operators and support
functions belong to pg_catalog; and every key column has a pg_catalog base
type (or a domain over one).

A `DEFERRABLE` constraint trigger, or a `DEFERRABLE EXCLUDE`, `UNIQUE` or
foreign key whose operator class, expression, collation or key type is
user-defined, refuses every write.
Disable such triggers (`ALTER TABLE ... DISABLE TRIGGER`) or keep them in
another database.

### Schema-qualified relations and the pinned search_path

A search_path switch that persists on the connection (an earlier
session-level `set_config(..., false)`, or a deferred trigger racing a host
REPEATABLE READ transaction) must not redirect a later Selecto statement to
another schema's relation, function or operator of the same name. So:

- **Every relation of a generated read or write is schema-qualified.** A
  domain relation declares its schema with `source_schema` next to its
  `source_table` (`source_schema: "pg_temp"` for a host temp table), and a
  write command with a `"schema.relation"` relation. Any other name is
  resolved against the connection's **reset** search_path
  (`pg_settings.reset_val`: the server, database, role or startup value,
  which `SET` and `set_config` do not change), which PostgreSQL itself
  parses: inside a savepoint or transaction of its own that is rolled back,
  the lookup applies it with `set_config(..., true)` and reads
  `current_schemas(false)`, so quoting, case folding, `"$user"`
  (`current_user`) and missing schemas follow the server, and `pg_temp`
  never takes part. The name is also looked up under the session's
  **current** path: a relation the two resolve differently refuses
  (`reason: :relation_ambiguous`) instead of being redirected, and a
  relation only the session's temp schema holds refuses unless declared
  (`reason: :relation_unresolved`). **A host that `SET`s search_path to
  reach another schema must declare that schema** (or set it at connection
  startup, for example `parameters: [search_path: "app"]`). A name that
  does not resolve, or a lookup that cannot run, refuses; and the exact
  text every Selecto read, stream, write or candidate load executes must
  name each relation schema-qualified, so no unqualified statement runs. A
  Selecto read that reaches the adapter without prepared relations resolves
  the names in its own text the same way.
- A CTE or query member named like a real relation is scoped: in a leading
  non-recursive `WITH`, its own body and the bodies before it read the real
  (qualified) relation, the later bodies and the main query the CTE. A
  collision that cannot be scoped (`WITH RECURSIVE`, a nested `WITH`)
  refuses (`reason: :cte_name_collision`).
- **Each Selecto statement runs under pinned settings**: search_path
  `pg_catalog, <every schema of the reset search_path>, <declared schemas>,
  pg_temp`, and `TimeZone`,
  `DateStyle`, `IntervalStyle`, `extra_float_digits`, `bytea_output`,
  `standard_conforming_strings` and `timezone_abbreviations` each at its
  `pg_settings.reset_val` (the
  server, database, role or startup value; an unreadable one refuses), set
  in one statement with
  `set_config(..., true)` inside the read's savepoint or transaction, or the
  write's, and restored to the host's values afterwards in the same
  expression that checks every pin held. A session value a trigger or
  earlier SQL persisted (even one masked by a local value of the same
  setting) therefore never decides how a Selecto statement resolves names
  or parses and renders values. A stream holds its pins on its own
  connection checkout until it ends. `lc_monetary` and
  `lc_numeric` are not pinned: generated SQL does not format money or
  locale numbers. Unqualified functions and operators
  therefore resolve to pg_catalog's first, then as the host's reset path
  resolves them (an extension's types and operators included), and temp
  objects last.
- **Explicit operators.** A search_path pin cannot stop an operator in a
  pinned schema that matches the operand types exactly (`public.=(dom, dom)`
  for a domain column, `=(varchar, varchar)`, an enum's own `=`): PostgreSQL
  prefers an exact match to pg_catalog's. So every comparison Selecto
  executes names its operator: `(CAST(col AS T) OPERATOR(s.op) CAST($n AS T))`,
  where `s` is pg_catalog for built-in, domain, enum, range, array and
  composite types and the type's own schema for an extension type (citext),
  and `T` the operator's argument type (a domain's base type, text for
  varchar, the type itself otherwise), read from the catalog for every column
  of the read's relations. IN and NOT IN become disjunctions of equalities,
  BETWEEN a pair of bounds, IN (subquery) `= ANY`, LIKE an explicit `~~`
  over `like_escape`, and joins, scope keys, tenant pairs, association pins,
  retarget membership, writes, foreign-key guards, MERGE and the post-image
  re-read name pg_catalog's operator too. Compile-only SQL (`to_sql/1`
  without a connection) is unchanged.
- **One last step before every statement runs** (R47): every function the
  statement calls without a schema is qualified with the schema PostgreSQL
  would pick under the pinned search_path (pg_catalog's when it has one,
  so a `public.lower(varchar)` never shadows it; else the extension's own
  schema), a comparison of two atoms that a builder wrote bare (recursive
  query-member joins, a retarget's context correlation) becomes
  pg_catalog's operator, and a statement that would still resolve an
  operator or function by name is refused (`reason: :bare_operator`).
  Builders that write compound bare comparisons (tagging, hierarchy, OLAP,
  enhanced joins, subfilters) are therefore refused on PostgreSQL rather
  than run. The test suite records every statement the adapter generates
  and fails if any of them would resolve by name.
- Every catalog read the adapter runs for its checks is pg_catalog-qualified.

## Atomic write graphs and MERGE

The adapter executes `Selecto.Write.Graph` inside one native transaction. It
resolves generated parent keys, enforces every row cardinality, and rolls back
the complete graph on any ownership or statement failure.

Owned-set sync automatically selects the strongest safe strategy supported by
the connected server:

| Server | Strategy |
| --- | --- |
| PostgreSQL 17+ | One relation-level `MERGE` with `RETURNING`, `merge_action()`, and `WHEN NOT MATCHED BY SOURCE` delete-missing |
| PostgreSQL 15–16 | Ordered parameterized updates/inserts plus identity-safe delete-missing in the same transaction |
| Older/unknown | The same ordered atomic fallback; no MERGE claim is advertised |

Pure nested inserts continue to use `INSERT … RETURNING`; they are not forced
through `MERGE`. Updato does not expose a strategy switch. Capability reporting
includes `merge`, `merge_returning`, and `merge_delete_missing` so diagnostics
can explain the selected path.

## Stream cancellation

Early stream termination sends a cooperative cancellation message and waits
for the cursor worker to unwind before using a bounded forced shutdown. This
preserves the checked-out PostgreSQL session instead of unnecessarily killing
and reconnecting it. The connected stream regression checks the same backend
PID and a session-local temporary table after cancellation.

Run `mix test --include postgres` for connected adapter tests. The sibling
SPA API's `scripts/verify_postgres_exports.exs` additionally exercises exact
scalar exports, early cancellation, disk-backed worksheet rollover and the
canonical API engine helper against synthetic session-local data.

## Local Workspace Development

For local multi-repo workspace development, set:

```bash
SELECTO_ECOSYSTEM_USE_LOCAL=true
```

When enabled, this package resolves a local path for `selecto`.

For a non-local build, set `SELECTO_ECOSYSTEM_USE_LOCAL=0`. The current interim
profile resolves Selecto from an exact GitHub commit; public Hex publication is
deferred.
