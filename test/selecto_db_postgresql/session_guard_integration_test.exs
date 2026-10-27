defmodule SelectoDBPostgreSQL.SessionGuardIntegrationTest do
  # Session state a statement can change for the statements after it
  # (brief items 51, 51a, 54, 54a, 54b, 56, 56b, 56c, 59, 61). A row
  # trigger, or a function a domain view calls, can switch search_path or
  # TimeZone with set_config(..., true) or create a TEMP relation or type,
  # which PostgreSQL resolves before the search path for the rest of the
  # session. Every Selecto write and read runs between two session snapshots
  # and refuses (and rolls back) any change; a temp relation or type that
  # shadows a name a statement uses is refused.
  use ExUnit.Case, async: false

  alias Selecto.Write.{Command, Error}
  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  setup do
    suffix = System.unique_integer([:positive])
    schema = "selecto_session_#{suffix}"
    evil = "selecto_session_evil_#{suffix}"
    options = SelectoDBPostgreSQL.Verification.ConnectionOptions.options()
    {:ok, setup} = Adapter.connect(Keyword.put(options, :pool_size, 1))
    exec!(setup, "CREATE SCHEMA #{schema}")
    exec!(setup, "CREATE SCHEMA #{evil}")
    GenServer.stop(setup)

    # The host's schema is its connection's startup search_path (the reset
    # value Selecto resolves relations against, R29), not a session SET.
    {:ok, connection} =
      Adapter.connect(
        options
        |> Keyword.put(:pool_size, 1)
        |> Keyword.put(:parameters, search_path: schema)
      )

    Process.unlink(connection)

    on_exit(fn ->
      if Process.alive?(connection) do
        Adapter.execute(connection, "DISCARD TEMP", [], [])
        Adapter.execute(connection, "DROP SCHEMA IF EXISTS #{schema} CASCADE", [], [])
        Adapter.execute(connection, "DROP SCHEMA IF EXISTS #{evil} CASCADE", [], [])
        GenServer.stop(connection)
      end
    end)

    exec!(connection, "CREATE TABLE items (id int PRIMARY KEY, org_key text NOT NULL, note text)")

    exec!(
      connection,
      "INSERT INTO items VALUES (1, 't1', 'mine'), (2, 't2', 'SECRET-t2'), (3, 't10', 'SECRET-t10')"
    )

    exec!(connection, "CREATE TABLE other (id int PRIMARY KEY, note text)")
    exec!(connection, "INSERT INTO other VALUES (1, 'x')")

    # A decoy that answers for `items` once search_path names the evil
    # schema first: every row in tenant t1.
    exec!(
      connection,
      "CREATE TABLE #{evil}.items (id int PRIMARY KEY, org_key text NOT NULL, note text)"
    )

    exec!(connection, "INSERT INTO #{evil}.items VALUES (1, 't1', 'decoy'), (9, 't1', 'decoy')")

    %{connection: connection, schema: schema, evil: evil}
  end

  defp exec!(connection, sql), do: {:ok, _} = Adapter.execute(connection, sql, [], [])

  defp rows!(connection, sql) do
    {:ok, %{rows: rows}} = Adapter.execute(connection, sql, [], [])
    rows
  end

  defp trigger!(connection, table, timing, body) do
    exec!(
      connection,
      "CREATE OR REPLACE FUNCTION #{table}_fn() RETURNS trigger LANGUAGE plpgsql AS " <>
        "$$ BEGIN #{body} RETURN NEW; END $$"
    )

    exec!(
      connection,
      "CREATE TRIGGER #{table}_tr #{timing} ON #{table} FOR EACH ROW EXECUTE FUNCTION #{table}_fn()"
    )
  end

  defp update(relation, assignments, scope \\ nil) do
    predicate =
      if scope,
        do: {:and, [{:eq, {:field, "id"}, {:literal, 1}}, scope]},
        else: {:eq, {:field, "id"}, {:literal, 1}}

    metadata = if scope, do: %{post_image: %{predicate: scope, key: ["id"]}}, else: %{}

    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: relation,
        predicate: predicate,
        assignments: Enum.map(assignments, fn {f, v} -> %{field: f, value: {:literal, v}} end),
        expected_cardinality: {:exactly, 1},
        metadata: metadata
      })

    command
  end

  defp tenant(value), do: {:eq, {:field, "org_key"}, {:literal, value}}

  defp changed?({:error, %Error{details: %{code: :query_rule_violation} = details}}),
    do: details.reason == :session_state_changed

  defp changed?(_result), do: false

  defp domain(table) do
    %{
      name: table,
      source: %{
        source_table: table,
        primary_key: :id,
        tenant_field: :org_key,
        fields: [:id, :org_key, :note],
        redact_fields: [],
        columns: %{id: %{type: :integer}, org_key: %{type: :string}, note: %{type: :string}},
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  defp read(connection, table \\ "items") do
    domain(table)
    |> Selecto.configure(connection, adapter: Adapter, validate: false, rollup_sort_fix: false)
    |> Selecto.with_tenant("t1")
    |> Selecto.select(["id", "note"])
    |> Selecto.order_by(["id"])
    |> Selecto.execute()
  end

  defp read_code({:ok, {rows, _columns, _aliases}}), do: rows
  defp read_code({:error, %{details: %{reason: reason}}}), do: reason

  test "a guarded write whose trigger switches search_path to a decoy is refused",
       %{connection: c, evil: evil} do
    trigger!(
      c,
      "items",
      "BEFORE UPDATE",
      "NEW.org_key := 't2'; PERFORM set_config('search_path', '#{evil}, ' || current_setting('search_path'), true);"
    )

    assert changed?(
             Adapter.execute_write_unsafe(c, update("items", [{"note", "u"}], tenant("t1")), [])
           )

    assert rows!(c, "SELECT org_key, note FROM items WHERE id = 1") == [["t1", "mine"]]
  end

  test "in a host transaction an unscoped write's search_path switch is rolled back and refused",
       %{connection: c, evil: evil, schema: schema} do
    trigger!(
      c,
      "other",
      "BEFORE UPDATE",
      "PERFORM set_config('search_path', '#{evil}, #{schema}', true);"
    )

    {:ok, outcome} =
      Postgrex.transaction(c, fn conn ->
        write = Adapter.execute_write_unsafe(conn, update("other", [{"note", "y"}]), [])
        {write, read(conn), rows!(conn, "SELECT pg_catalog.current_setting('search_path')")}
      end)

    {write, next_read, [[search_path]]} = outcome
    assert changed?(write)
    assert read_code(next_read) == [[1, "mine"]]
    assert search_path == schema
    assert rows!(c, "SELECT note FROM other") == [["x"]]
  end

  test "a trigger's TimeZone or DateStyle switch refuses the write", %{connection: c} do
    trigger!(
      c,
      "items",
      "AFTER UPDATE",
      "PERFORM set_config('TimeZone', 'Asia/Kathmandu', true);"
    )

    assert changed?(
             Adapter.execute_write_unsafe(c, update("items", [{"note", "tz"}], tenant("t1")), [])
           )

    exec!(c, "DROP TRIGGER items_tr ON items")
    trigger!(c, "items", "AFTER UPDATE", "PERFORM set_config('DateStyle', 'SQL, DMY', true);")
    assert changed?(Adapter.execute_write_unsafe(c, update("items", [{"note", "ds"}]), []))
    assert rows!(c, "SELECT note FROM items WHERE id = 1") == [["mine"]]
  end

  test "a trigger-created TEMP view or type refuses the write and does not survive",
       %{connection: c} do
    trigger!(
      c,
      "other",
      "BEFORE UPDATE",
      "EXECUTE 'CREATE TEMP VIEW items AS SELECT id, ''t1''::text AS org_key, note FROM items';"
    )

    assert changed?(Adapter.execute_write_unsafe(c, update("other", [{"note", "y"}]), []))
    assert rows!(c, "SELECT pg_catalog.to_regclass('pg_temp.items')::text") == [[nil]]
    assert read_code(read(c)) == [[1, "mine"]]

    exec!(
      c,
      "CREATE OR REPLACE FUNCTION other_fn() RETURNS trigger LANGUAGE plpgsql AS " <>
        "$$ BEGIN EXECUTE 'CREATE DOMAIN pg_temp.text AS varchar(2)'; RETURN NEW; END $$"
    )

    assert changed?(Adapter.execute_write_unsafe(c, update("other", [{"note", "y"}]), []))
  end

  test "renaming an existing temp table onto a domain relation is refused", %{connection: c} do
    exec!(c, "CREATE TEMP TABLE scratch (id int PRIMARY KEY, org_key text NOT NULL, note text)")

    trigger!(
      c,
      "other",
      "BEFORE UPDATE",
      "INSERT INTO pg_temp.scratch SELECT id, 't1', note FROM items; " <>
        "EXECUTE 'ALTER TABLE pg_temp.scratch RENAME TO items';"
    )

    assert changed?(Adapter.execute_write_unsafe(c, update("other", [{"note", "y"}]), []))
    assert read_code(read(c)) == [[1, "mine"]]
  end

  test "a temp relation or type shadowing a domain name refuses reads, streams and writes",
       %{connection: c} do
    # A temp relation shadowing a domain relation resolves it differently
    # under the session's path (R38: relation_ambiguous); a shadow the
    # lookup cannot see (a temp type) is the shadow check's (item 56).
    exec!(c, "CREATE TEMP VIEW items AS SELECT id, 't1'::text AS org_key, note FROM items")
    shadowed = [:temporary_shadow, :relation_ambiguous]
    assert read_code(read(c)) in shadowed

    assert {:error, %Error{details: %{reason: reason}}} =
             Adapter.execute_write_unsafe(c, update("items", [{"note", "p"}], tenant("t1")), [])

    assert reason in shadowed

    stream =
      domain("items")
      |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
      |> Selecto.with_tenant("t1")
      |> Selecto.select(["id", "note"])
      |> Selecto.execute_stream()

    case stream do
      {:ok, rows} ->
        assert_raise RuntimeError, ~r/shadows a name the statement uses/, fn ->
          Enum.to_list(rows)
        end

      {:error, %{details: %{reason: reason}}} ->
        assert reason in shadowed
    end

    assert rows!(c, "SELECT note FROM items ORDER BY id") |> length() == 3

    exec!(c, "DROP VIEW pg_temp.items")
    exec!(c, "CREATE DOMAIN pg_temp.text AS varchar(2)")
    assert read_code(read(c)) == :temporary_shadow
  end

  test "host temp tables keep working when declared", %{connection: c} do
    # An unrelated temp table, and a domain over a temp table that shadows
    # nothing (item 19): declared (source_schema / "pg_temp.name") it works;
    # undeclared it is refused (R38).
    exec!(c, "CREATE TEMP TABLE scratch (id int)")
    exec!(c, "CREATE TEMP TABLE tonly (id int PRIMARY KEY, org_key text NOT NULL, note text)")
    exec!(c, "INSERT INTO tonly VALUES (1, 't1', 'tmp'), (2, 't2', 'x')")

    assert read_code(read(c)) == [[1, "mine"]]
    assert read_code(read(c, "tonly")) == :relation_unresolved

    declared =
      domain("tonly")
      |> put_in([:source, :source_schema], "pg_temp")
      |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
      |> Selecto.with_tenant("t1")
      |> Selecto.select(["id", "note"])
      |> Selecto.execute()

    assert read_code(declared) == [[1, "tmp"]]

    assert {:error, %Error{details: %{reason: :relation_unresolved}}} =
             Adapter.execute_write_unsafe(c, update("tonly", [{"note", "t"}], tenant("t1")), [])

    assert {:ok, _} =
             Adapter.execute_write_unsafe(
               c,
               update("pg_temp.tonly", [{"note", "t"}], tenant("t1")),
               []
             )

    assert {:ok, _} =
             Adapter.execute_write_unsafe(c, update("items", [{"note", "m"}], tenant("t1")), [])
  end

  test "a read whose view function switches search_path is refused and leaves no trace",
       %{connection: c, evil: evil, schema: schema} do
    for scope <- ["true", "false"] do
      exec!(
        c,
        "CREATE OR REPLACE FUNCTION vf(i int) RETURNS text LANGUAGE plpgsql AS " <>
          "$$ BEGIN PERFORM set_config('search_path', '#{evil}, #{schema}', #{scope}); RETURN 'v'; END $$"
      )

      exec!(
        c,
        "CREATE OR REPLACE VIEW items_v AS SELECT id, org_key, note, vf(id) AS v FROM items"
      )

      # Inside the host's transaction the read rolls back to its savepoint.
      {:ok, {first, next}} =
        Postgrex.transaction(c, fn conn -> {read(conn, "items_v"), read(conn)} end)

      assert read_code(first) == :session_state_changed
      assert read_code(next) == [[1, "mine"]]

      # In autocommit the read runs in its own transaction under the pinned
      # search_path (R30): any switch, local or session-level, is seen and
      # rolled back with it.
      assert read_code(read(c, "items_v")) == :session_state_changed
      assert rows!(c, "SELECT pg_catalog.current_setting('search_path')") == [[schema]]
      assert read_code(read(c)) == [[1, "mine"]]
    end

    stream =
      domain("items_v")
      |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
      |> Selecto.with_tenant("t1")
      |> Selecto.select(["id", "note"])
      |> Selecto.execute_stream()

    assert {:ok, rows} = stream
    assert_raise RuntimeError, ~r/changed the session/, fn -> Enum.to_list(rows) end
    assert rows!(c, "SELECT pg_catalog.current_setting('search_path')") == [[schema]]
  end

  # R8, R23: a deferred constraint trigger fires at COMMIT, after the
  # post-DML snapshot; no write runs while a user deferrable trigger exists,
  # in autocommit or in a host transaction.
  test "no write runs while a user deferrable trigger exists",
       %{connection: c, evil: evil, schema: schema} do
    exec!(
      c,
      "CREATE OR REPLACE FUNCTION other_deferred() RETURNS trigger LANGUAGE plpgsql AS " <>
        "$$ BEGIN PERFORM set_config('search_path', '#{evil}, #{schema}', false); RETURN NULL; END $$"
    )

    exec!(
      c,
      "CREATE CONSTRAINT TRIGGER other_deferred_tr AFTER UPDATE ON other " <>
        "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION other_deferred()"
    )

    assert {:error, %Error{details: %{reason: :deferred_triggers}}} =
             Adapter.execute_write_unsafe(c, update("other", [{"note", "y"}]), [])

    # Unrelated relations too: the trigger could be on any table the write
    # reaches.
    assert {:error, %Error{details: %{reason: :deferred_triggers}}} =
             Adapter.execute_write_unsafe(c, update("items", [{"note", "m"}], tenant("t1")), [])

    assert rows!(c, "SELECT pg_catalog.current_setting('search_path')") == [[schema]]
    assert rows!(c, "SELECT note FROM other") == [["x"]]
    assert read_code(read(c)) == [[1, "mine"]]

    # Inside the host's transaction the trigger would fire at the host's
    # COMMIT: the write is refused.
    {:ok, outcome} =
      Postgrex.transaction(c, fn conn ->
        Adapter.execute_write_unsafe(conn, update("other", [{"note", "h"}]), [])
      end)

    assert {:error, %Error{details: %{reason: :deferred_triggers}}} = outcome
    assert rows!(c, "SELECT note FROM other") == [["x"]]
    exec!(c, "DROP TRIGGER other_deferred_tr ON other")
  end

  # R29: relations resolve against the reset search_path, never the current
  # one, so a session-level switch that persisted before Selecto's call
  # (an earlier set_config(..., false), a deferred trigger's racing switch)
  # redirects neither a read nor a write to the decoy.
  test "a persisted session search_path switch redirects no read or write",
       %{connection: c, evil: evil, schema: schema} do
    exec!(c, "SELECT pg_catalog.set_config('search_path', '#{evil}, #{schema}', false)")
    assert rows!(c, "SELECT note FROM items ORDER BY id") == [["decoy"], ["decoy"]]

    # The session's path resolves items to the decoy, the reset path to the
    # real table: refused (R38), never redirected.
    assert read_code(read(c)) == :relation_ambiguous

    assert {:error, %Error{details: %{reason: :relation_ambiguous}}} =
             Adapter.execute_write_unsafe(c, update("items", [{"note", "m2"}], tenant("t1")), [])

    assert rows!(c, "SELECT note FROM #{schema}.items WHERE id = 1") == [["mine"]]
    assert rows!(c, "SELECT note FROM #{evil}.items ORDER BY id") == [["decoy"], ["decoy"]]

    # Declared, the relation is named by its schema whatever the path.
    declared =
      domain("items")
      |> put_in([:source, :source_schema], schema)
      |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
      |> Selecto.with_tenant("t1")
      |> Selecto.select(["id", "note"])
      |> Selecto.execute()

    assert read_code(declared) == [[1, "mine"]]

    assert {:ok, %{affected_rows: 1}} =
             Adapter.execute_write_unsafe(
               c,
               update("#{schema}.items", [{"note", "m2"}], tenant("t1")),
               []
             )

    assert rows!(c, "SELECT note FROM #{schema}.items WHERE id = 1") == [["m2"]]
    # The host's own (switched) value is handed back unchanged.
    assert rows!(c, "SELECT pg_catalog.current_setting('search_path')") == [
             ["#{evil}, #{schema}"]
           ]
  end

  # R30: a persisted switch would also capture unqualified operators: an
  # exact-match =(varchar, text) in the decoy schema beats pg_catalog's
  # text = text for a varchar column. Selecto pins search_path to
  # pg_catalog, the domain's schemas and pg_temp around its statements.
  test "a persisted switch to a schema with a decoy operator does not capture the read",
       %{connection: c, evil: evil, schema: schema} do
    exec!(c, "CREATE TABLE vitems (id int PRIMARY KEY, org_key varchar NOT NULL, note text)")

    exec!(
      c,
      "INSERT INTO vitems VALUES (1, 't1', 'mine'), (2, 't2', 'SECRET-t2')"
    )

    exec!(
      c,
      "CREATE FUNCTION #{evil}.always(varchar, text) RETURNS boolean LANGUAGE sql " <>
        "IMMUTABLE AS 'SELECT true'"
    )

    exec!(
      c,
      "CREATE OPERATOR #{evil}.= (LEFTARG = varchar, RIGHTARG = text, FUNCTION = #{evil}.always)"
    )

    exec!(c, "SELECT pg_catalog.set_config('search_path', '#{evil}, #{schema}', false)")

    assert rows!(c, "SELECT id FROM vitems WHERE org_key = CAST('t1' AS text) ORDER BY id") ==
             [[1], [2]]

    assert read_code(read(c, "vitems")) == [[1, "mine"]]

    {:ok, rows} =
      domain("vitems")
      |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
      |> Selecto.with_tenant("t1")
      |> Selecto.select(["id", "note"])
      |> Selecto.execute_stream()

    assert [{[1, "mine"], ["id", "note"]}] = Enum.map(Enum.to_list(rows), &elem(&1, 0))

    assert rows!(c, "SELECT pg_catalog.current_setting('search_path')") == [
             ["#{evil}, #{schema}"]
           ]
  end

  # R29: a lookup that cannot run refuses; no unqualified statement runs.
  test "a relation the reset search_path does not reach is refused", %{connection: c} do
    assert read_code(read(c, "no_such_relation")) == :relation_unresolved
  end

  # R32: an internal recheck trigger of a DEFERRABLE EXCLUDE or UNIQUE whose
  # operator class calls user code runs it at COMMIT; it counts. Deferred
  # foreign keys and all-pg_catalog rechecks do not.
  test "a deferrable recheck that can run user code refuses writes; catalog rechecks and FKs do not",
       %{connection: c, schema: schema} do
    exec!(c, "CREATE TABLE dparent (id int PRIMARY KEY)")
    exec!(c, "INSERT INTO dparent VALUES (1)")

    exec!(
      c,
      "CREATE TABLE dchild (id int PRIMARY KEY, org_key text NOT NULL, note text UNIQUE " <>
        "DEFERRABLE INITIALLY DEFERRED, parent_id int REFERENCES dparent (id) " <>
        "DEFERRABLE INITIALLY DEFERRED)"
    )

    exec!(c, "INSERT INTO dchild VALUES (1, 't1', 'a', 1)")

    assert {:ok, %{affected_rows: 1}} =
             Adapter.execute_write_unsafe(c, update("dchild", [{"note", "b"}], tenant("t1")), [])

    exec!(
      c,
      "CREATE FUNCTION #{schema}.ev_cmp(a text, b text) RETURNS int LANGUAGE plpgsql IMMUTABLE " <>
        "AS $$ BEGIN RETURN pg_catalog.bttextcmp(a, b); END $$"
    )

    for {name, op} <- [lt: "<", le: "<=", eq: "=", ge: ">=", gt: ">"] do
      exec!(
        c,
        "CREATE FUNCTION #{schema}.ev_#{name}(a text, b text) RETURNS boolean LANGUAGE sql " <>
          "IMMUTABLE AS 'SELECT pg_catalog.bttextcmp(a, b) #{op} 0'"
      )
    end

    exec!(
      c,
      "CREATE OPERATOR #{schema}.<<< (LEFTARG = text, RIGHTARG = text, FUNCTION = #{schema}.ev_lt)"
    )

    exec!(
      c,
      "CREATE OPERATOR #{schema}.<<= (LEFTARG = text, RIGHTARG = text, FUNCTION = #{schema}.ev_le)"
    )

    exec!(
      c,
      "CREATE OPERATOR #{schema}.=== (LEFTARG = text, RIGHTARG = text, FUNCTION = #{schema}.ev_eq, " <>
        "COMMUTATOR = ===)"
    )

    exec!(
      c,
      "CREATE OPERATOR #{schema}.>>= (LEFTARG = text, RIGHTARG = text, FUNCTION = #{schema}.ev_ge)"
    )

    exec!(
      c,
      "CREATE OPERATOR #{schema}.>>> (LEFTARG = text, RIGHTARG = text, FUNCTION = #{schema}.ev_gt)"
    )

    exec!(
      c,
      "CREATE OPERATOR CLASS #{schema}.evil_ops FOR TYPE text USING btree AS OPERATOR 1 <<<, " <>
        "OPERATOR 2 <<=, OPERATOR 3 ===, OPERATOR 4 >>=, OPERATOR 5 >>>, FUNCTION 1 #{schema}.ev_cmp(text, text)"
    )

    exec!(
      c,
      "CREATE TABLE x_ex (id int PRIMARY KEY, k text, " <>
        "EXCLUDE USING btree (k #{schema}.evil_ops WITH ===) DEFERRABLE INITIALLY DEFERRED)"
    )

    # Only internal triggers exist: the R23 user-trigger count is zero.
    assert rows!(
             c,
             "SELECT count(*) FROM pg_trigger WHERE tgdeferrable AND NOT tgisinternal"
           ) == [[0]]

    assert {:error, %Error{details: %{reason: :deferred_triggers}}} =
             Adapter.execute_write_unsafe(c, update("dchild", [{"note", "c"}], tenant("t1")), [])

    assert rows!(c, "SELECT note FROM dchild") == [["b"]]

    exec!(c, "DROP TABLE x_ex")

    assert {:ok, %{affected_rows: 1}} =
             Adapter.execute_write_unsafe(c, update("dchild", [{"note", "c"}], tenant("t1")), [])

    # R37: a deferred FK whose key type is user-defined (a composite here)
    # runs user-type comparison code at COMMIT; R32a: a deferrable UNIQUE
    # under a user collation does too.
    for {setup, teardown} <- [
          {[
             "CREATE TYPE #{schema}.kpair AS (a int, b int)",
             "CREATE TABLE kparent (k #{schema}.kpair PRIMARY KEY)",
             "CREATE TABLE kchild (k #{schema}.kpair REFERENCES kparent (k) DEFERRABLE INITIALLY DEFERRED)"
           ], ["DROP TABLE kchild", "DROP TABLE kparent", "DROP TYPE #{schema}.kpair"]},
          {[
             "CREATE COLLATION #{schema}.ucoll (provider = libc, locale = 'C')",
             "CREATE TABLE ucol (k text COLLATE #{schema}.ucoll UNIQUE DEFERRABLE INITIALLY DEFERRED)"
           ], ["DROP TABLE ucol", "DROP COLLATION #{schema}.ucoll"]}
        ] do
      Enum.each(setup, &exec!(c, &1))

      assert {:error, %Error{details: %{reason: :deferred_triggers}}} =
               Adapter.execute_write_unsafe(
                 c,
                 update("dchild", [{"note", "d"}], tenant("t1")),
                 []
               ),
             inspect(setup)

      Enum.each(teardown, &exec!(c, &1))
    end

    assert {:ok, %{affected_rows: 1}} =
             Adapter.execute_write_unsafe(c, update("dchild", [{"note", "e"}], tenant("t1")), [])
  end

  # R35 (amended): a TimeZone or DateStyle the session persisted before
  # Selecto's call (masked by a local value of the same setting inside a
  # transaction, so no before/after compare sees it) never decides how a
  # Selecto statement renders or parses a value: every statement runs under
  # each setting's reset value, here a database-level Asia/Karachi and
  # 'SQL, DMY', and the host's values are handed back.
  test "a persisted or masked TimeZone/DateStyle does not decide a scope compare" do
    prefix = System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")
    database = "#{prefix}_tz_#{System.unique_integer([:positive])}"
    options = SelectoDBPostgreSQL.Verification.ConnectionOptions.options()
    {:ok, admin} = Adapter.connect(Keyword.put(options, :pool_size, 1))
    exec!(admin, ~s|CREATE DATABASE "#{database}"|)
    exec!(admin, ~s|ALTER DATABASE "#{database}" SET TimeZone = 'Asia/Karachi'|)
    exec!(admin, ~s|ALTER DATABASE "#{database}" SET DateStyle = 'SQL, DMY'|)

    try do
      {:ok, c} =
        Adapter.connect(options |> Keyword.put(:pool_size, 1) |> Keyword.put(:database, database))

      exec!(c, "CREATE TABLE ev (id int PRIMARY KEY, org_key text NOT NULL, ts timestamptz)")

      exec!(
        c,
        "INSERT INTO ev VALUES (1, 't1', '2024-01-01 02:00:00+00'), " <>
          "(2, 't1', '2023-12-31 20:00:00+00')"
      )

      # A text rendering of the instant depends on TimeZone and DateStyle.
      exec!(
        c,
        "CREATE VIEW ev_v AS SELECT id, org_key, CAST(ts AS text) AS ts_text, " <>
          "current_setting('TimeZone') AS tz, current_setting('DateStyle') AS ds FROM ev"
      )

      read = fn select, filters ->
        %{
          name: "ev_v",
          source: %{
            source_table: "ev_v",
            primary_key: :id,
            fields: [:id, :org_key, :ts_text, :tz, :ds],
            redact_fields: [],
            columns: %{
              id: %{type: :integer},
              org_key: %{type: :string},
              ts_text: %{type: :string},
              tz: %{type: :string},
              ds: %{type: :string}
            },
            associations: %{}
          },
          schemas: %{},
          joins: %{},
          required_filters: filters
        }
        |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
        |> Selecto.select(select)
        |> Selecto.order_by(["id"])
        |> Selecto.execute()
        |> read_code()
      end

      # The database's own values render row 1 as 01/01/2024 07:00:00 PKT
      # and row 2 as 01/01/2024 01:00:00 PKT.
      assert read.(["tz", "ds"], [{"id", 1}]) == [["Asia/Karachi", "SQL, DMY"]]
      assert read.(["id"], [{"ts_text", {:gte, "01/01/2024"}}]) == [[1], [2]]

      # Masked: the session value is changed, the transaction-local one is
      # the database's; after COMMIT the changed session value persists.
      {:ok, _} =
        Postgrex.transaction(c, fn conn ->
          Postgrex.query!(conn, "SELECT set_config('TimeZone', 'UTC', false)", [])
          Postgrex.query!(conn, "SELECT set_config('DateStyle', 'ISO, MDY', false)", [])
          Postgrex.query!(conn, "SELECT set_config('TimeZone', 'Asia/Karachi', true)", [])
          Postgrex.query!(conn, "SELECT set_config('DateStyle', 'SQL, DMY', true)", [])
        end)

      assert rows!(c, "SELECT current_setting('TimeZone')") == [["UTC"]]

      # The statement still runs under the database's values, and renders
      # (and compares) row 1 as it did before the switch.
      assert read.(["tz", "ds"], [{"id", 1}]) == [["Asia/Karachi", "SQL, DMY"]]
      assert read.(["id"], [{"ts_text", "01/01/2024 07:00:00 PKT"}]) == [[1]]

      assert rows!(c, "SELECT current_setting('TimeZone'), current_setting('DateStyle')") ==
               [["UTC", "ISO, MDY"]]

      GenServer.stop(c)
    after
      exec!(admin, ~s|DROP DATABASE IF EXISTS "#{database}" WITH (FORCE)|)
      GenServer.stop(admin)
    end
  end

  # R36: PostgreSQL parses the reset search_path itself; a quoted "q,r"
  # schema is never split into a decoy `q`.
  test "a quoted reset search_path resolves as PostgreSQL parses it" do
    suffix = System.unique_integer([:positive])
    real = "selecto_q#{suffix},r"
    decoy = "selecto_q#{suffix}"
    options = SelectoDBPostgreSQL.Verification.ConnectionOptions.options()
    {:ok, admin} = Adapter.connect(Keyword.put(options, :pool_size, 1))
    exec!(admin, ~s|CREATE SCHEMA "#{real}"|)
    exec!(admin, ~s|CREATE SCHEMA "#{decoy}"|)

    for {schema, note} <- [{real, "mine"}, {decoy, "decoy"}] do
      exec!(
        admin,
        ~s|CREATE TABLE "#{schema}".qitems (id int PRIMARY KEY, org_key text, note text)|
      )

      exec!(admin, ~s|INSERT INTO "#{schema}".qitems VALUES (1, 't1', '#{note}')|)
    end

    try do
      {:ok, c} =
        Adapter.connect(
          options
          |> Keyword.put(:pool_size, 1)
          |> Keyword.put(:parameters, search_path: ~s|"#{real}"|)
        )

      assert read_code(read(c, "qitems")) == [[1, "mine"]]
      GenServer.stop(c)
    after
      exec!(admin, ~s|DROP SCHEMA "#{real}" CASCADE|)
      exec!(admin, ~s|DROP SCHEMA "#{decoy}" CASCADE|)
      GenServer.stop(admin)
    end
  end
end
