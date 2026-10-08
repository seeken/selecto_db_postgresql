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
