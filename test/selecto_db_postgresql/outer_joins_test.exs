defmodule SelectoDBPostgreSQL.OuterJoinsTest do
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  defmodule Pg18MockRepo do
    def query("show server_version_num", []), do: {:ok, %{rows: [["180001"]]}}
  end

  test "adapter reports right and full joins" do
    assert Adapter.supports?(:right_join)
    assert Adapter.supports?(:full_join)
  end

  test "a right join filters the target in a derived table; ON pairs the sides" do
    {sql, params, _} =
      %{type: :right, alias: "buyer_row"}
      |> domain("oj_orders", "oj_customers")
      |> Selecto.configure(Pg18MockRepo, adapter: Adapter, validate: false)
      |> Selecto.select(["id", "customer.name"])
      |> Selecto.gen_sql([])
      |> then(fn {sql, aliases, params} -> {sql, params, aliases} end)

    assert sql =~
             ~r/right join \(select \* from "?oj_customers"? "?buyer_row"? where "?buyer_row"?\."active" = \$1\) "?buyer_row"? on "?buyer_row"?\."id" = "?selecto_root"?\."customer_id"/

    assert params == [1]
  end

  describe "live" do
    @describetag :postgres

    setup do
      {:ok, conn} = Postgrex.start_link(ConnectionOptions.options())
      Process.unlink(conn)
      on_exit(fn -> if Process.alive?(conn), do: GenServer.stop(conn) end)

      suffix = System.unique_integer([:positive])
      orders = "selecto_oj_orders_#{suffix}"
      customers = "selecto_oj_customers_#{suffix}"

      for sql <- [
            "CREATE TEMP TABLE #{orders} (id INTEGER PRIMARY KEY, customer_id INTEGER, region TEXT)",
            "CREATE TEMP TABLE #{customers} (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)",
            "INSERT INTO #{orders} VALUES (1, 1, 'east'), (2, 2, 'east'), (3, NULL, 'west'), (4, 9, 'west')",
            "INSERT INTO #{customers} VALUES (1, 'ann', 1), (2, 'bob', 0), (3, 'cy', 1), (4, 'di', 0)"
          ],
          do: Postgrex.query!(conn, sql, [])

      %{conn: conn, orders: orders, customers: customers}
    end

    test "right and full joins keep unmatched rows after the target's where", ctx do
      assert rows(ctx, %{type: :right}) == Enum.sort([[1, "ann"], [nil, "cy"]])

      assert rows(ctx, %{type: :full, alias: "buyer_row"}) ==
               Enum.sort([[1, "ann"], [2, nil], [3, nil], [4, nil], [nil, "cy"]])

      assert rows(ctx, %{type: :full}, required_filters: [{"region", "east"}]) ==
               [[1, "ann"], [2, nil]]
    end
  end

  defp rows(ctx, join, opts \\ []) do
    domain = join |> domain(ctx.orders, ctx.customers) |> Map.merge(Map.new(opts))

    assert {:ok, {rows, _columns, _aliases}} =
             domain
             |> Selecto.configure(ctx.conn, adapter: Adapter, validate: false)
             |> Selecto.select(["id", "customer.name"])
             |> Selecto.execute(analyze_complexity: false)

    Enum.sort(rows)
  end

  defp domain(join, orders, customers) do
    %{
      name: "Orders",
      source: %{
        source_table: orders,
        primary_key: :id,
        fields: [:id, :customer_id, :region],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          customer_id: %{type: :integer},
          region: %{type: :string}
        },
        associations: %{
          customer: %{
            queryable: :customers,
            field: :customer,
            owner_key: :customer_id,
            related_key: :id,
            where: %{"active" => 1}
          }
        }
      },
      schemas: %{
        customers: %{
          source_table: customers,
          primary_key: :id,
          fields: [:id, :name, :active],
          redact_fields: [],
          columns: %{id: %{type: :integer}, name: %{type: :string}, active: %{type: :integer}},
          associations: %{}
        }
      },
      joins: %{customer: join}
    }
  end
end
