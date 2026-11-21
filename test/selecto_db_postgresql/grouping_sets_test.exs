defmodule SelectoDBPostgreSQL.GroupingSetsTest do
  use ExUnit.Case, async: false

  alias Selecto.Expr, as: X
  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  defmodule Pg18MockRepo do
    def query("show server_version_num", []), do: {:ok, %{rows: [["180001"]]}}
  end

  test "adapter reports cube and grouping sets" do
    assert Adapter.supports?(:cube)
    assert Adapter.supports?(:grouping_sets)
  end

  test "CUBE and GROUPING SETS compile natively with positional orderings" do
    base = Selecto.configure(domain(), Pg18MockRepo, adapter: Adapter, validate: false)

    {sql, _aliases, []} =
      base
      |> measures(["region", "channel"])
      |> Selecto.group_by(X.cube(["region", "channel"]))
      |> Selecto.order_by([{{:grouping, ["region", "channel"]}, :desc}])
      |> Selecto.gen_sql([])

    assert sql =~ ~r/group by\s+CUBE\(selecto_root\."?region"?, selecto_root\."?channel"?\)/
    assert sql =~ ~r/order by\s+5 desc/
    refute sql =~ "rollupfix"

    {sql, _aliases, []} =
      base
      |> measures(["region", "channel"])
      |> Selecto.group_by(X.grouping_sets([["region"], ["channel"], []]))
      |> Selecto.gen_sql([])

    assert sql =~ "GROUPING SETS ("
    assert sql =~ "())"
  end

  describe "live" do
    @describetag :postgres

    setup do
      {:ok, conn} = Postgrex.start_link(ConnectionOptions.options())
      Process.unlink(conn)
      on_exit(fn -> if Process.alive?(conn), do: GenServer.stop(conn) end)

      table = "selecto_gs_test_#{System.unique_integer([:positive])}"

      Postgrex.query!(
        conn,
        "CREATE TEMP TABLE #{table} (id INTEGER PRIMARY KEY, region TEXT, channel TEXT, amount INTEGER)",
        []
      )

      Postgrex.query!(
        conn,
        "INSERT INTO #{table} VALUES (1,'east','web',10),(2,'east','web',20),(3,'east','store',5)," <>
          "(4,'west','web',7),(5,'west',NULL,3),(6,NULL,'store',40)",
        []
      )

      domain = put_in(domain(), [:source, :source_table], table)
      %{base: Selecto.configure(domain, conn, adapter: Adapter, validate: false)}
    end

    test "a cube tells NULL keys from subtotals", %{base: base} do
      assert {:ok, {rows, _, _}} =
               base
               |> measures(["region", "channel"])
               |> Selecto.group_by(X.cube(["region", "channel"]))
               |> Selecto.execute()

      assert Enum.sort(rows) ==
               Enum.sort([
                 ["east", "web", 2, 30, 0],
                 ["east", "store", 1, 5, 0],
                 ["west", "web", 1, 7, 0],
                 ["west", nil, 1, 3, 0],
                 [nil, "store", 1, 40, 0],
                 ["east", nil, 3, 35, 1],
                 ["west", nil, 2, 10, 1],
                 [nil, nil, 1, 40, 1],
                 [nil, "web", 3, 37, 2],
                 [nil, "store", 2, 45, 2],
                 [nil, nil, 1, 3, 2],
                 [nil, nil, 6, 85, 3]
               ])
    end

    test "grouping sets ordered and limited", %{base: base} do
      assert {:ok, {rows, _, _}} =
               base
               |> measures(["region", "channel"])
               |> Selecto.group_by(X.grouping_sets([["region"], ["channel"], []]))
               |> Selecto.order_by([
                 {{:grouping, ["region", "channel"]}, :asc},
                 {{:sum, "amount"}, :desc}
               ])
               |> Selecto.limit(4)
               |> Selecto.execute()

      assert rows == [
               [nil, nil, 1, 40, 1],
               ["east", nil, 3, 35, 1],
               ["west", nil, 2, 10, 1],
               [nil, "store", 2, 45, 2]
             ]
    end
  end

  defp measures(query, dimensions) do
    Selecto.select(
      query,
      dimensions ++
        [
          X.as(X.count(), "n"),
          X.as({:sum, "amount"}, "total"),
          X.as({:grouping, dimensions}, "marker")
        ]
    )
  end

  defp domain do
    %{
      name: "sales",
      source: %{
        source_table: "sales",
        primary_key: :id,
        fields: [:id, :region, :channel, :amount],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          region: %{type: :string},
          channel: %{type: :string},
          amount: %{type: :integer}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end
end
