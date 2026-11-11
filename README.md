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

## Prepared statements

Every statement runs unnamed, always, so `pg_prepared_statements` stays empty
on Selecto's connections. Code a statement reaches (a trigger, a planted
function) can DEALLOCATE and PREPARE a named statement, in a READ ONLY
transaction and past a ROLLBACK, and the driver binds the replacement without
parsing it, so a named guard or business statement could be swapped. The
`statement_cache` setting is gone. Transaction statements use the simple
protocol, one round trip; the others take two (Parse/Describe, Bind/Execute).
This works behind a transaction-mode connection pooler.

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
before the statement. The statement returns the (tableoid, ctid) of every row
it wrote, and those exact rows are re-read after every row trigger has run,
inside the same transaction, from the schema-qualified table that oid named
and only among that table's (and its partitions') oids. It is rolled back
unless every written row is still inside the scope, so keys of any type work
and a trigger that renumbers a row behind a decoy cannot pass; a row a later
trigger updates again has a new row id and is refused. The check refuses
(`query_rule_not_evaluable`) whatever it cannot see through:

- Statement-level triggers and rewrite rules for the event on the table or
  any of its partitions, views, tables with inheritance (non-partition)
  children, and any enabled non-internal deferrable trigger in the database:
  it fires at COMMIT, after the check.
- A bound-scope DELETE (`metadata.bound_scope`, graph delete-missing
  cleanups included) locks its table ROW EXCLUSIVE and is refused, before
  and again after its statement, under an enabled statement-level DELETE
  trigger or a DELETE rule on the table or anywhere in its partition or
  inheritance tree, an enabled non-internal deferrable trigger, and on a
  view. A graph descendant bound by its root or an ancestor (an insert,
  update or delete on a table without a tenant field) follows the same rule
  for its event.
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

## Reads

A Selecto read runs on one checked-out connection, and the server's
transaction status decides its shape (`DBConnection.status/1`, so a
transaction opened with a raw text `BEGIN` is the host's too):

- **No transaction open:** the read is a transaction of its own: `BEGIN`, one
  statement that makes it `READ ONLY`, sets the read's `statement_timeout`
  and checks that `client_encoding` is UTF8, the query, `ROLLBACK`. Nothing
  it reaches can write and nothing it changes outlasts it: a view using
  `nextval` or an audit function refuses (`query_rule_not_evaluable`,
  `reason: :read_only_read`). A generated read that locks rows (`FOR UPDATE`,
  `FOR SHARE`) is not read-only; its locks end with it. A `ROLLBACK` that
  fails closes the connection, so the holder's next statement says so.
  `config :selecto_db_postgresql, read_only_reads: false` turns `READ ONLY`
  off for generated reads.
- **Inside the host's transaction:** the read is the host's own statement.
  One statement checks `client_encoding` and sets the read's
  `statement_timeout` local to the transaction, the query runs as written,
  and the prior `statement_timeout` is put back. Its locks, snapshot and any
  write a view makes belong to that transaction. A read that fails (a
  timeout, a missing relation) leaves the transaction aborted for the host
  to roll back; Selecto never sends `COMMIT` or `ROLLBACK` for a transaction
  it does not own, and kills no process on such a connection.
- **Timeouts.** A read's `:timeout` is the server's `statement_timeout`; a
  shorter `statement_timeout` of the host's stands. The connection's own
  limit comes a few seconds later, for a server that does not answer at all.
- **Statements are Selecto's or the host's.** A statement is Selecto's only
  when its executor stamped it (`selecto_generated`, or `selecto_raw_sql` for
  one carrying a host fragment: a custom column's `select:`, a
  `raw_sql_filter`, a `{:ref, text}`, an `{:exists, sql}`). Anything else
  given to a read path is hand-built: the host's own code. **Never build
  hand-built text or a raw fragment from end-user input.** Hand-built text
  must be one statement beginning `SELECT`, `WITH`, `VALUES`, `TABLE`, `SHOW`
  or `EXPLAIN` (`reason: :hand_built_not_a_read`). With no transaction open
  it runs as written, under `READ ONLY`. In the host's transaction nothing
  would stop a write, so text whose words could write (`INSERT`, `UPDATE`,
  `DELETE`, `MERGE`, `TRUNCATE` or `INTO`, as a data-modifying `WITH` or
  `SELECT INTO` uses, and `EXPLAIN ANALYZE` of one) refuses before anything
  is sent (`reason: :hand_built_in_host_transaction`); a row lock is
  allowed, and so are sequences and the host's own functions and views. The
  check reads words, not meaning (a column named `update` refuses too; a
  quoted `"delete"` or one in a string literal does not): it stops honest
  mistakes, not evasion.
- **Declared schemas.** Selecto compiles relation names plainly. A relation
  a domain declares with `source_schema` next to its `source_table` is
  written `"schema".relation` in the statement a read runs (and in its
  stream and EXPLAIN); a write names it with a `"schema.relation"` relation.
  A nested query (subquery join, CTE, LATERAL) whose domain declares no
  schema takes its enclosing query's declared root, as an ad-hoc join's
  table does, and a retargeted query its origin's. The statement names a
  relation once, so a name read in two schemas, or declared in one query of
  the statement and not in another, refuses (`reason: :relation_ambiguous`).
  Nothing is read from the catalog for this. A relation no domain declares
  a schema for resolves by the session's `search_path`.
- **Lists.** Within a Selecto statement a name read as a table is never also
  a CTE or VALUES list's name: a table named like a list defined in the
  statement refuses (`reason: :cte_name_collision`), as does one name
  defined twice with different lists (`reason: :cte_name_redefined`). A
  query's root reads a list of its name when its domain declares no
  `source_schema` and the query defines that list. A raw CTE
  (`{:raw_cte, sql, params}`) is the host's own text: plain
  `name [(columns)] AS [NOT] [MATERIALIZED] (...)` definitions whose `$n`
  placeholders its params bind exactly; anything else refuses
  (`raw_cte_unparsed`, `raw_cte_params_mismatch`).
- **Streams** follow the same rules in a transaction of their own: a
  connection in the host's transaction, or a session that is not UTF8, is
  refused at the call (`reason: :stream_in_transaction`,
  `:client_encoding`).
- **EXPLAIN is a read.** `Selecto.explain/2`, `explain_analyze/2` and
  `QueryAnalyzer` run exactly as the read itself would.
- **`Selecto.to_sql/2` and `gen_sql/2` are compile-only**, without declared
  schemas; run a read through `Selecto.execute/2`.
- **uuid columns** declared `:uuid` take their text value as a uuid; a write
  binds a uuid column's text (a domain over uuid included) whatever its
  field type, and text that is no uuid refuses (`reason: :invalid_uuid`), as
  does a non-canonical spelling on a tenant column.

## Writes

A governed write runs in a transaction the adapter opens (READ COMMITTED),
or under a savepoint of its own in the host's transaction, which a refused
write rolls back. Before each command one statement reads its facts:
`client_encoding` (UTF8, or `reason: :client_encoding`), the transaction's
isolation level, and the target's uuid columns. Generated write SQL names
its operators and functions plainly and its relation as the command names
it. A post-image re-read and the statement-trigger, rule and deferrable
checks address the relation by the oid and schema-qualified name resolved
under its lock before the DML (see the post-image check above).

## What the host must provide

Selecto's statements run in the host's session, under its settings, and
resolve names as the host's own queries do. The host provides:

- **`client_encoding` UTF8.** Bound values are UTF-8; read as another
  encoding a tenant name with an accent is another tenant's. It is checked
  by every read, stream and write.
- **A `search_path` that names the application's schemas**, or
  `source_schema` on the domains. An extension (`citext`) must be in a
  schema on the path for its operators to apply.
- **No untrusted role with `CREATE` on a schema on the path.** An operator
  or function created there (an exact-type `=` over `varchar`, a
  `lower(varchar)`) would be chosen over `pg_catalog`'s, for Selecto's
  statements as for any other. Triggers, views and row-security policies
  must schema-qualify the functions they call, and must not change session
  state (`search_path`, temporary relations) that later statements rely on.
- **Transactions through a handle** (`Postgrex.transaction/3`,
  `DBConnection.run/3`, an Ecto repository). A raw text `BEGIN` is seen and
  respected, but a pool can reconnect between statements and the host's
  following statements would autocommit.

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
