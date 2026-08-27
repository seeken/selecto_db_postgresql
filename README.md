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

## Usage

Pass the adapter explicitly when configuring Selecto:

```elixir
selecto =
  Selecto.configure(domain, pg_opts,
    adapter: SelectoDBPostgreSQL.Adapter
  )
```

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

## Notes

- Placeholder style is `$N`.
- Identifier quoting uses double quotes.
- Pool-backed execution delegates to `Selecto.ConnectionPool`.

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

## Local Workspace Development

For local multi-repo workspace development, set:

```bash
SELECTO_ECOSYSTEM_USE_LOCAL=true
```

When enabled, this package resolves a local path for `selecto`.

For a non-local build, set `SELECTO_ECOSYSTEM_USE_LOCAL=0`. The current interim
profile resolves Selecto from an exact GitHub commit; public Hex publication is
deferred.
