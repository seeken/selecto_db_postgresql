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
  `source_table` (`source_schema: "pg_temp"` for a host temp table): reads
  and SelectoUpdato writes of that domain both name it. A hand-built write
  command names it with a `"schema.relation"` relation. Any other name is
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
  (`reason: :relation_unresolved`): declare `source_schema: "pg_temp"` on the
  domain that names it. An ad-hoc join cannot name a schema: its table takes
  its root's, so it reaches a temporary table only under a root that declares
  `pg_temp`; otherwise read it through `join_subquery` on a domain that
  declares `pg_temp`.
  **A host that `SET`s search_path to
  reach another schema must declare that schema** (or set it at connection
  startup, for example `parameters: [search_path: "app"]`). A name that
  does not resolve, or a lookup that cannot run, refuses; and the exact
  text every Selecto read, stream, write or candidate load executes must
  name each relation schema-qualified, so no unqualified statement runs.
  Hand-built reads are the exception, below.
- **Each query of a statement resolves under its own declarations.** An
  ad-hoc join's table, a subquery join's query, a CTE's or LATERAL join's
  query and a set operation's members are resolved, pinned and guarded like
  the domain's own tables. A nested query (subquery join, CTE, LATERAL) whose
  domain declares no `source_schema` takes its enclosing query's declared
  root, as an ad-hoc join's table does (R59), and a retargeted query that
  declares none takes its origin's; one that declares its own keeps it. Set
  operation members are siblings and take nothing from each other.
  The SQL names a relation once, so a name two of them would resolve to two
  relations refuses (`reason: :relation_ambiguous`) rather than read one for
  both. Within a Selecto statement a name read as a table is never also a CTE or
  VALUES list's name. A name a query reads is a reference to a list at two
  places only: a query's root, when its domain declares no `source_schema` and
  the query defines a list of that name itself (or, for a CTE body, one
  visible where the body sits), which is how to select from a CTE; and an
  ad-hoc join naming a visible list. Every other name is a table: joined
  schemas, through tables, declared non-association joins, closure and
  junction tables, and every table of a query the statement embeds (filter and
  CASE subqueries, `{:exists, query}`, `{:subquery, ...}`), a subquery or
  LATERAL join, a set member, a recursive base or step, or a retarget origin.
  A table named like a list defined anywhere in the statement refuses
  (`reason: :cte_name_collision`), and one name defined twice with different
  lists refuses (`reason: :cte_name_redefined`) unless the two sit in
  different members of one set operation, each private to its member. A root
  that reads a list named like a relation on the session's search_path refuses
  too (`reason: :cte_name_collision`): which one was meant cannot be told. An
  ad-hoc join naming a visible list reads the list whatever tables exist.
  Every table is resolved under its query's declarations (an undeclared
  subquery or declared join takes its root's declared schema, R59) and
  qualified like the domain's own; a table two queries would resolve to two
  relations refuses (`reason: :relation_ambiguous`). The CTEs Selecto
  generates start with `__selecto_` and are named by their content (a values
  list's rows, a hierarchy join's definition), within 63 characters. A
  recursive CTE whose base or recursive query defines its own CTE or VALUES
  list refuses (`reason: :recursive_cte_nested_with`); a copy of the
  statement's VALUES list or of a CTE emitted before it is the statement's and
  is allowed. A relation named by a declared or inherited schema that does not
  exist there refuses (`reason: :relation_unresolved`) before the read runs.
- A raw CTE (`{:raw_cte, sql, params}`, a binary or iodata with
  `{:param, value}` markers) is the host's own text (R68): it must be plain
  `name [(columns)] AS [NOT] [MATERIALIZED] (...)` definitions, comments
  (nested ones too), dollar quotes and "double-quoted" or unquoted names
  allowed. Every name it defines counts in the rule above like any list's, the
  list-root check included; its bodies are not read. Text that does not parse
  to the end refuses (`reason: :raw_cte_unparsed`), as do a backslash in a
  string literal, non-ASCII names, SEARCH/CYCLE, a RECURSIVE prefix and a
  trailing comma. Names compare case-insensitively. A list's name is written
  unquoted when plain (letters, digits, underscores, not reserved) in its
  definition and every root and join naming it, so it folds as the host's own
  SQL naming it unquoted does; other names, and a raw CTE's quoted names, are
  quoted. The list-root check looks a root up by that identifier.
- **Each Selecto statement runs under pinned settings**: search_path
  `pg_catalog, <declared schemas>, <every schema of the reset search_path>,
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
- **One last step before every statement runs** (R47, R49c, R50):
  `SelectoDBPostgreSQL.Qualify` rewrites whatever a builder wrote bare, so the
  specialised builders (tagging, hierarchy, OLAP, enhanced joins) run like
  any other. Every operator names its schema, grouped as PostgreSQL's
  precedence groups it; IN, BETWEEN, LIKE, SIMILAR TO, IS DISTINCT FROM,
  NULLIF and simple CASE get explicit operators; every function is
  pg_catalog's (a `public.lower(varchar)` never shadows it), the function an
  extension defines for its first argument's type (citext's `max`,
  `replace`), or the one candidate among the pinned schemas. The operands of
  an operator or function in an extension's schema are cast to the member's
  argument type, so nothing planted beside it over a domain is chosen. A
  citext against text compares as text, in either order, as PostgreSQL
  resolves it. An expression that may have an extension type but cannot be
  resolved by query scope (`COALESCE(citext_col, 'x') = $1`) refuses
  (`reason: :operand_type_unknown`), and so does a function with no single
  candidate (`:function_not_found`, `:function_ambiguous`). Under REPEATABLE
  READ or SERIALIZABLE the types of the columns a statement names are checked
  through the parser (`pg_typeof((NULL::rel).col)`), and a change after the
  transaction's snapshot refuses (`:catalog_changed`). A statement already
  explicit runs as it is, with no catalog read. The test suite records every
  statement at the driver call and fails if any of Selecto's own, guard and
  catalog SQL included, would resolve by name; the host's SQL is never
  rewritten.
- **Extension operators are named only as members.** An extension's schema
  is commonly writable, and an operator planted there resolves as readily as
  one of the extension's own. So an operator outside pg_catalog is written
  only when it is a `pg_depend` member of the operand type's extension for
  those operand types; `hstore < hstore`, which the extension does not define,
  refuses (`reason: :operator_not_member`) whatever is planted beside it, and
  an `OPERATOR(ext.op)` already in a statement is verified the same way.
  pg_catalog's operator stands in for a missing member in exactly one case: a
  string-category extension type (citext, domains over it) against a
  pg_catalog string type, which compares as text as PostgreSQL does. Any other
  pair refuses in Selecto, not at the server: an implicit cast could make it
  resolve and compare the wrong thing (box3d `=` by area, geometry as text).
- **Hand-built SQL.** Text wholly the host's runs exactly as written, in the host's
  session: no qualification, no rewriting, no catalog proof, no search_path or
  settings pin and no relation guard, so its answers are the driver's, under the
  host's search_path, `SET LOCAL`s and temporary tables. A statement is generated
  only when Selecto's executor stamped it (`selecto_generated`). Three things still
  apply: the text must be a read by its first word (SELECT, WITH, VALUES, TABLE,
  SHOW, EXPLAIN, or `reason: :hand_built_not_a_read`), it always runs READ ONLY,
  whatever `read_only_reads` says, so what it would write refuses
  (`reason: :read_only_read`, SQLSTATE 25006), and a session that is not UTF8
  refuses. So `transaction_read_only` reads `on` inside such a read.
- **Host fragments.** A statement Selecto built that carries a host fragment, a
  custom column's `select:`, a `raw_sql_filter`, a `{:ref, text}` or an
  `{:exists, sql}` say, stays generated: its relations, filters and tenant scope
  keep their pin and guards, and declared schemas hold. Only the fragment is
  verbatim: no operator of such a statement is rewritten, and it runs READ ONLY.
  Under the pin a fragment's bare names resolve as the pin does, not as the host's
  search_path would. Tenant scope depends on a `raw_sql_filter` being well-formed:
  one that closes its own parenthesis escapes the scope.
- **Never build hand-built text or a raw fragment from end-user input.** It is the
  host's own code: the write check stops honest mistakes, not evasion.
- **Row locks.** Hand-built text and raw text cannot take row locks, on any
  route: `FOR UPDATE`, `FOR NO KEY UPDATE`, `FOR SHARE` and `FOR KEY SHARE` refuse
  `:read_only_read`. Take row locks through your own driver.
- **Deployment.** No untrusted role may have CREATE on a schema the application
  reaches, and hosts schema-qualify functions in their triggers, views and
  policies: code already running as the application role is outside what these
  guards address.
- **Functions.** Selecto does not reimplement overload resolution. A function
  outside pg_catalog is named only as the exact match for its arguments' known
  types or the sole candidate that can take them, and no argument is cast to a
  type it lacks; anything else is `:function_ambiguous`, as is a non-member
  beside an extension's own functions. A function no extension owns, bare or
  schema-qualified, refuses (`:function_not_member`) unless the domain declares
  it as a UDF. A word before `(` is a function unless its position proves syntax
  (hstore's `delete`, `cube(x, y)`; `GROUP BY CUBE (x, y)` is syntax), and
  `U&"..."` identifiers are decoded first.
- **Reads cannot write.** A Selecto read runs with `transaction_read_only`
  pinned on, so a function or planted operator it reaches cannot write through
  it; in a host transaction its savepoint ends by ROLLBACK TO, which restores
  the host's setting. A generated read that takes row locks (`FOR UPDATE`,
  `FOR SHARE`) stays writable, since PostgreSQL refuses those read-only. A host function
  that writes (or `nextval`) in a domain view or custom column therefore
  refuses in a read (`reason: :read_only_read`). None of this costs the host its
  transaction: before the savepoint a read takes the relation locks the host's
  own SELECT would hold to its commit, through a view to its base tables too, and
  with them the transaction's first snapshot. A hand-built statement is planned
  for that (`EXPLAIN (FORMAT JSON)`, under the read's deadline), and a plan that
  would write or lock rows keeps nothing, since the read is about to refuse. A
  relation only a function a view calls reaches is locked only for the read, and
  so is `pg_advisory_xact_lock`: the read's ROLLBACK TO releases it. Take advisory
  locks through your own driver. Planning can evaluate a function declared
  IMMUTABLE or STABLE, outside READ ONLY mode: one that writes breaks PostgreSQL's
  contract for those words.
- **Timeouts.** A read's `:timeout` is the server's `statement_timeout`, local to
  the read's own transaction or savepoint: the reply is consumed, the read is
  undone, and a transaction the host holds goes on. Selecto kills no process on
  such a connection. The connection's own limit comes a few seconds later, for a
  server that does not answer at all. Should a read's undo itself fail, the
  connection closes and the holder's next statement says so; Selecto never sends
  ROLLBACK or COMMIT for a transaction it does not own. A transaction begun with
  text `BEGIN` on a pool pid is host misuse: a pool can reconnect between statements
  with or without Selecto, and the host's following statements would autocommit.
  Use a transaction handle (`DBConnection.run/3` or `Postgrex.transaction/3`).
  `config :selecto_db_postgresql, read_only_reads: false` turns this off.
- **EXPLAIN is a read.** `Selecto.explain/2`, `explain_analyze/2` and
  `QueryAnalyzer` run through the same qualification, pin and session checks,
  with the same refusals: even plain EXPLAIN evaluates operators, since a
  selectivity estimator such as `eqsel` applies them to most-common values.
- **`Selecto.to_sql/2` and `gen_sql/2` are compile-only.** Their text names
  relations and operators bare and has none of this protection. It is for
  display and tests; run a read through `Selecto.execute/2`.
- **uuid columns** take their text value as a uuid in reads and writes,
  whatever the field's declared type. Text that is no uuid refuses
  (`reason: :invalid_uuid`), as does a non-canonical spelling on a tenant
  column.
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
