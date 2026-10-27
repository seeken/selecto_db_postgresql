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
    {:ok, connection} =
      Adapter.connect(
        Keyword.put(SelectoDBPostgreSQL.Verification.ConnectionOptions.options(), :pool_size, 1)
      )

    Process.unlink(connection)
    suffix = System.unique_integer([:positive])
    schema = "selecto_session_#{suffix}"
    evil = "selecto_session_evil_#{suffix}"
    exec!(connection, "CREATE SCHEMA #{schema}")
    exec!(connection, "CREATE SCHEMA #{evil}")
    exec!(connection, "SET search_path TO #{schema}")

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
    exec!(c, "CREATE TEMP VIEW items AS SELECT id, 't1'::text AS org_key, note FROM items")
    assert read_code(read(c)) == :temporary_shadow

    assert {:error, %Error{details: %{reason: :temporary_shadow}}} =
             Adapter.execute_write_unsafe(c, update("items", [{"note", "p"}], tenant("t1")), [])

    stream =
      domain("items")
      |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
      |> Selecto.with_tenant("t1")
      |> Selecto.select(["id", "note"])
      |> Selecto.execute_stream()

    assert {:ok, rows} = stream
    assert_raise RuntimeError, ~r/shadows a name the statement uses/, fn -> Enum.to_list(rows) end

    exec!(c, "DROP VIEW pg_temp.items")
    exec!(c, "CREATE DOMAIN pg_temp.text AS varchar(2)")
    assert read_code(read(c)) == :temporary_shadow
  end

  test "host temp tables keep working", %{connection: c} do
    # An unrelated temp table, and a domain over a temp table that shadows
    # nothing (item 19).
    exec!(c, "CREATE TEMP TABLE scratch (id int)")
    exec!(c, "CREATE TEMP TABLE tonly (id int PRIMARY KEY, org_key text NOT NULL, note text)")
    exec!(c, "INSERT INTO tonly VALUES (1, 't1', 'tmp'), (2, 't2', 'x')")

    assert read_code(read(c)) == [[1, "mine"]]
    assert read_code(read(c, "tonly")) == [[1, "tmp"]]

    assert {:ok, _} =
             Adapter.execute_write_unsafe(c, update("tonly", [{"note", "t"}], tenant("t1")), [])

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

      # In autocommit a transaction-local change ends with the statement; a
      # session-level one is restored to the settings the read saw before.
      expected = if scope == "false", do: :session_state_changed, else: [[1, "mine"]]
      assert read_code(read(c, "items_v")) == expected
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

  # R8: a deferred constraint trigger fires at COMMIT, after the post-DML
  # snapshot; its session-level set_config would outlive the write.
  test "a deferred trigger's session change is seen before the write commits",
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

    assert changed?(Adapter.execute_write_unsafe(c, update("other", [{"note", "y"}]), []))
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
end
