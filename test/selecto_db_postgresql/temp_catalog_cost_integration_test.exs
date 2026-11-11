defmodule SelectoDBPostgreSQL.TempCatalogCostIntegrationTest do
  # Every Selecto read and write fingerprints the session's temp catalog
  # (SessionGuard) and counts its temp relations (RelationPath). pg_class and
  # pg_type have no index on the namespace alone, so each such match scanned
  # the whole catalog: about 30 ms a read on a 300k-relation catalog. A
  # session with no temp schema (pg_my_temp_schema() is 0) has nothing to
  # find, and a one-time filter now skips those scans; a session with temp
  # objects scans and detects them exactly as before.
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.{Adapter, SessionGuard}

  @moduletag :postgres

  setup do
    suffix = System.unique_integer([:positive])
    schema = "selecto_tempcat_#{suffix}"
    options = SelectoDBPostgreSQL.Verification.ConnectionOptions.options()

    # A fresh session: no temp object has been made in it yet.
    {:ok, connection} =
      Adapter.connect(
        options
        |> Keyword.put(:pool_size, 1)
        |> Keyword.put(:parameters, search_path: schema)
      )

    Process.unlink(connection)
    exec!(connection, "CREATE SCHEMA #{schema}")
    exec!(connection, "CREATE TABLE #{schema}.items (id int PRIMARY KEY, note text)")
    exec!(connection, "INSERT INTO #{schema}.items VALUES (1, 'mine'), (2, 'other')")

    on_exit(fn ->
      if Process.alive?(connection) do
        Adapter.execute(connection, "DISCARD TEMP", [], [])
        Adapter.execute(connection, "DROP SCHEMA IF EXISTS #{schema} CASCADE", [], [])
        GenServer.stop(connection)
      end
    end)

    %{connection: connection}
  end

  defp exec!(connection, sql), do: {:ok, _} = Adapter.execute(connection, sql, [], [])

  defp rows!(connection, sql) do
    {:ok, %{rows: rows}} = Adapter.execute(connection, sql, [], [])
    rows
  end

  # The catalog scans of the session fingerprint, from its executed plan.
  defp catalog_scans(connection) do
    connection
    |> rows!(
      "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) " <> SessionGuard.snapshot_sql()
    )
    |> Enum.map(&hd/1)
    |> Enum.filter(&(&1 =~ ~r/Scan on pg_(class|type|rewrite)\b/))
  end

  defp read(connection, domain_fields \\ [id: :integer, note: :string]) do
    %{
      name: "items",
      schemas: %{},
      joins: %{},
      source: %{
        source_table: "items",
        primary_key: :id,
        fields: Keyword.keys(domain_fields),
        redact_fields: [],
        columns: Map.new(domain_fields, fn {k, v} -> {k, %{type: v}} end),
        associations: %{}
      }
    }
    |> Selecto.configure(connection, adapter: Adapter, validate: false, rollup_sort_fix: false)
    |> Selecto.select(["id", "note"])
    |> Selecto.filter({"id", 1})
    |> Selecto.execute()
    |> case do
      {:ok, {rows, _columns, _aliases}} -> rows
      {:error, %Selecto.Error{details: details}} -> details[:reason]
    end
  end

  test "the temp-catalog scans run only in a session with a temp schema, and detect as before",
       %{connection: c} do
    # No temp schema: the scans never run, the fingerprint holds no temp
    # object, and reads answer.
    assert rows!(c, "SELECT CAST(pg_catalog.pg_my_temp_schema() AS pg_catalog.int8)") == [[0]]
    scans = catalog_scans(c)
    assert scans != []
    assert Enum.all?(scans, &(&1 =~ "never executed")), Enum.join(scans, "\n")
    assert rows!(c, SessionGuard.snapshot_sql()) |> hd() |> hd() == "temp | end"
    assert read(c) == [[1, "mine"]]

    # A temp table named like the domain's relation: the session now has a
    # temp schema, the scans run and see it, and it captures nothing (the
    # relation is read by its own schema, R38a).
    exec!(c, "CREATE TEMP TABLE items (id int PRIMARY KEY, note text)")
    exec!(c, "INSERT INTO pg_temp.items VALUES (1, 'temp decoy')")
    refute rows!(c, "SELECT CAST(pg_catalog.pg_my_temp_schema() AS pg_catalog.int8)") == [[0]]
    scans = catalog_scans(c)
    refute Enum.any?(scans, &(&1 =~ "never executed")), Enum.join(scans, "\n")

    [[fingerprint | _]] = rows!(c, SessionGuard.snapshot_sql())

    [[oid]] =
      rows!(c, "SELECT CAST(CAST('pg_temp.items' AS pg_catalog.regclass) AS pg_catalog.oid)")

    assert fingerprint =~ ~r/[ ,]#{oid}:\d+:items[ ,]/
    assert read(c) == [[1, "mine"]]

    # A temp type named like a type the generated SQL casts to is refused.
    exec!(c, "CREATE DOMAIN pg_temp.text AS varchar(2)")
    assert read(c) == :temporary_shadow
  end

  test "a session whose temp objects were dropped skips the scans; a new one is detected",
       %{connection: c} do
    # A pooled connection keeps its temp schema after its temp objects go.
    exec!(c, "CREATE TEMP TABLE scratch (id int PRIMARY KEY, note text)")
    exec!(c, "CREATE TEMP VIEW scratch_v AS SELECT id FROM pg_temp.scratch")
    exec!(c, "CREATE DOMAIN pg_temp.small AS int")
    exec!(c, "DROP VIEW pg_temp.scratch_v")
    exec!(c, "DROP TABLE pg_temp.scratch")
    exec!(c, "DROP DOMAIN pg_temp.small")
    refute rows!(c, "SELECT CAST(pg_catalog.pg_my_temp_schema() AS pg_catalog.int8)") == [[0]]

    scans = catalog_scans(c)
    assert scans != []
    assert Enum.all?(scans, &(&1 =~ "never executed")), Enum.join(scans, "\n")
    assert rows!(c, SessionGuard.snapshot_sql()) |> hd() |> hd() == "temp | end"
    assert read(c) == [[1, "mine"]]

    # Anything created in it again is scanned and seen as before, a type
    # alone included.
    exec!(c, "CREATE DOMAIN pg_temp.text AS varchar(2)")
    refute Enum.any?(catalog_scans(c), &(&1 =~ "never executed"))
    assert rows!(c, SessionGuard.snapshot_sql()) |> hd() |> hd() =~ ~r/:text( |,)/
    assert read(c) == :temporary_shadow
  end
end
