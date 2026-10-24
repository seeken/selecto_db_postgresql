defmodule SelectoDBPostgreSQL.ColumnCatalogIntegrationTest do
  # column_catalog/2 reports each column's complete type with its typmod and
  # the collation's determinism (query_enforced_scalar v2 verified metadata).
  # SelectoUpdato's in-process candidate check decides by it, so a missing
  # typmod or a nondeterministic collation reported as deterministic would
  # admit rows PostgreSQL stores differently.
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  setup do
    {:ok, connection} =
      Adapter.connect(SelectoDBPostgreSQL.Verification.ConnectionOptions.options())

    Process.unlink(connection)

    on_exit(fn ->
      if Process.alive?(connection), do: GenServer.stop(connection)
    end)

    %{connection: connection}
  end

  test "types carry their typmod; collations their determinism; enums their base", %{
    connection: connection
  } do
    suffix = System.unique_integer([:positive])
    ci = "selecto_ci_#{suffix}"
    ci_bin = "selecto_ci_bin_#{suffix}"
    mood = "selecto_mood_#{suffix}"
    table = "selecto_catalog_#{suffix}"

    for sql <- [
          "CREATE COLLATION #{ci} (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
          "CREATE COLLATION #{ci_bin} (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
          "CREATE TYPE #{mood} AS ENUM ('a', 'b')",
          """
          CREATE TABLE #{table} (
            id bigint PRIMARY KEY, org_key varchar(2), body text, ci_key text COLLATE #{ci},
            bin_key text COLLATE #{ci_bin}, c_key text COLLATE "C", price numeric(10,2),
            free numeric, at0 timestamp(0), at timestamp, mood #{mood}
          )
          """
        ],
        do: {:ok, _} = Adapter.execute(connection, sql, [], [])

    try do
      {:ok, columns} = Adapter.column_catalog(connection, table)

      assert columns["org_key"] == %{type: "character varying(2)", deterministic: true}
      assert columns["body"] == %{type: "text", deterministic: true}
      assert columns["ci_key"] == %{type: "text", deterministic: false}
      assert columns["bin_key"] == %{type: "text", deterministic: false}
      assert columns["c_key"] == %{type: "text", deterministic: true}
      assert columns["price"].type == "numeric(10,2)"
      assert columns["free"].type == "numeric"
      assert columns["at0"].type == "timestamp(0) without time zone"
      assert columns["at"].type == "timestamp without time zone"
      assert columns["mood"].type == "enum"
      assert columns["id"].type == "bigint"

      # An unknown table reports nothing (the check then fails closed).
      assert {:ok, %{}} = Adapter.column_catalog(connection, "selecto_no_such_#{suffix}")
    after
      for sql <- [
            "DROP TABLE IF EXISTS #{table}",
            "DROP TYPE IF EXISTS #{mood}",
            "DROP COLLATION IF EXISTS #{ci}",
            "DROP COLLATION IF EXISTS #{ci_bin}"
          ],
          do: Adapter.execute(connection, sql, [], [])
    end
  end
end
