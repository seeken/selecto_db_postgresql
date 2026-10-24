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

  test "the relation resolves exactly as the write quotes it", %{connection: connection} do
    suffix = System.unique_integer([:positive])
    schema = "selecto_cat_s_#{suffix}"
    mixed = "Cat_Case_#{suffix}"
    folded = String.downcase(mixed)
    quoted = ~s(we"ird_#{suffix})
    plain = "cat_q_#{suffix}"
    quote = &Adapter.quote_identifier/1

    statements = [
      "CREATE TABLE #{quote.(mixed)} (org_key varchar(2))",
      "CREATE TABLE #{folded} (org_key text)",
      "CREATE TABLE #{quote.(quoted)} (org_key varchar(3))",
      "CREATE SCHEMA #{schema}",
      "CREATE TABLE #{schema}.#{plain} (org_key varchar(4))",
      "CREATE TABLE #{plain} (org_key text)"
    ]

    for sql <- statements, do: {:ok, _} = Adapter.execute(connection, sql, [], [])

    try do
      assert {:ok, %{"org_key" => %{type: "character varying(2)"}}} =
               Adapter.column_catalog(connection, mixed)

      assert {:ok, %{"org_key" => %{type: "text"}}} = Adapter.column_catalog(connection, folded)

      assert {:ok, %{"org_key" => %{type: "character varying(3)"}}} =
               Adapter.column_catalog(connection, quoted)

      assert {:ok, %{"org_key" => %{type: "character varying(4)"}}} =
               Adapter.column_catalog(connection, "#{schema}.#{plain}")

      assert {:ok, %{"org_key" => %{type: "text"}}} = Adapter.column_catalog(connection, plain)

      # A temp table shadows the permanent one, for the catalog as for the
      # unqualified INSERT.
      {:ok, _} =
        Adapter.execute(connection, "CREATE TEMP TABLE #{plain} (org_key varchar(5))", [], [])

      assert {:ok, %{"org_key" => %{type: "character varying(5)"}}} =
               Adapter.column_catalog(connection, plain)

      {:ok, _} = Adapter.execute(connection, "DROP TABLE pg_temp.#{plain}", [], [])
    after
      for sql <- [
            "DROP TABLE IF EXISTS #{quote.(mixed)}",
            "DROP TABLE IF EXISTS #{folded}",
            "DROP TABLE IF EXISTS #{quote.(quoted)}",
            "DROP TABLE IF EXISTS #{plain}",
            "DROP SCHEMA IF EXISTS #{schema} CASCADE"
          ],
          do: Adapter.execute(connection, sql, [], [])
    end
  end

  test "domains resolve to their base type and typmod; an unresolved chain is base_verified",
       %{connection: connection} do
    suffix = System.unique_integer([:positive])
    d = &"selecto_cat_d#{&1}_#{suffix}"
    table = "selecto_cat_dom_#{suffix}"
    chain = for level <- 1..17, do: d.("chain#{level}")

    statements =
      [
        "CREATE DOMAIN #{d.("vc2")} AS varchar(2)",
        "CREATE DOMAIN #{d.("num")} AS numeric(10,0)",
        "CREATE DOMAIN #{d.("ts")} AS timestamp(0)",
        "CREATE DOMAIN #{d.("dd")} AS #{d.("vc2")}",
        "CREATE DOMAIN #{hd(chain)} AS varchar(2)"
      ] ++
        (chain
         |> Enum.chunk_every(2, 1, :discard)
         |> Enum.map(fn [base, next] -> "CREATE DOMAIN #{next} AS #{base}" end)) ++
        [
          "CREATE TABLE #{table} (k #{d.("vc2")}, n #{d.("num")}, at #{d.("ts")}, " <>
            "kk #{d.("dd")}, deep #{List.last(chain)})"
        ]

    for sql <- statements, do: {:ok, _} = Adapter.execute(connection, sql, [], [])

    try do
      {:ok, columns} = Adapter.column_catalog(connection, table)
      assert columns["k"] == %{type: "character varying(2)", deterministic: true}
      assert columns["n"].type == "numeric(10,0)"
      assert columns["at"].type == "timestamp(0) without time zone"
      assert columns["kk"] == %{type: "character varying(2)", deterministic: true}
      # Seventeen levels exceed the walk: reported unresolved, base_verified.
      assert columns["deep"].trust == :base_verified
      refute columns["deep"].type =~ "character varying"
    after
      Adapter.execute(connection, "DROP TABLE IF EXISTS #{table}", [], [])

      for name <- Enum.reverse(chain) ++ [d.("dd"), d.("vc2"), d.("num"), d.("ts")],
          do: Adapter.execute(connection, "DROP DOMAIN IF EXISTS #{name}", [], [])
    end
  end
end
