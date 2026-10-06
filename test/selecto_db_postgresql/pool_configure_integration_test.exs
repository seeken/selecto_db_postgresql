defmodule SelectoDBPostgreSQL.PoolConfigureIntegrationTest do
  @moduledoc """
  A managed pool configured through `Selecto.configure/3`, either passed as
  `{:pool, reference}` or started with `pool: true`, stays a pool reference
  the adapter accepts: configure's automatic `rollup_sort_fix` reads the
  server version through it, and `Selecto.execute/2` runs on it.
  """

  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  @moduletag :postgres

  setup_all do
    {:ok, conn} = Postgrex.start_link(ConnectionOptions.options())
    items = "selecto_pool_configure_items_#{System.unique_integer([:positive])}"

    Postgrex.query!(conn, "CREATE TABLE #{items} (id integer PRIMARY KEY, name text)", [])

    Postgrex.query!(
      conn,
      "INSERT INTO #{items} SELECT g, 'item ' || g FROM generate_series(1, 25) g",
      []
    )

    %{rows: [[version_num]]} = Postgrex.query!(conn, "show server_version_num", [])
    GenServer.stop(conn)

    on_exit(fn ->
      {:ok, cleanup} = Postgrex.start_link(ConnectionOptions.options())
      Postgrex.query!(cleanup, "DROP TABLE IF EXISTS #{items}", [])
      GenServer.stop(cleanup)
    end)

    # PostgreSQL 18 sorts ROLLUP output itself; older servers need the fix.
    {:ok, items: items, rollup_sort_fix: String.to_integer(version_num) < 180_000}
  end

  test "configure keeps a {:pool, reference} and executes on it",
       %{items: items, rollup_sort_fix: rollup_sort_fix} do
    {:ok, pool_ref} =
      Selecto.ConnectionPool.start_pool(ConnectionOptions.options(),
        adapter: Adapter,
        pool_size: 2,
        max_overflow: 0
      )

    on_exit(fn -> Selecto.ConnectionPool.stop_pool(pool_ref) end)
    pool = {:pool, pool_ref}

    selecto = Selecto.configure(domain(items), pool, adapter: Adapter)

    assert selecto.connection == pool
    assert selecto.runtime.connection == pool
    assert selecto.config.rollup_sort_fix == rollup_sort_fix

    assert_executes(selecto)

    assert_executes(
      Selecto.configure(domain(items), pool, adapter: Adapter, rollup_sort_fix: false)
    )
  end

  test "pool: true starts a managed pool and executes on it",
       %{items: items, rollup_sort_fix: rollup_sort_fix} do
    selecto =
      Selecto.configure(domain(items), ConnectionOptions.options(),
        adapter: Adapter,
        pool: true,
        pool_options: [pool_size: 2, max_overflow: 0]
      )

    assert {:pool, %{adapter: Adapter, pool: pool_pid}} = selecto.connection
    assert is_pid(pool_pid)
    on_exit(fn -> Selecto.ConnectionPool.stop_pool(selecto.connection) end)

    assert selecto.config.rollup_sort_fix == rollup_sort_fix
    assert_executes(selecto)
  end

  defp assert_executes(selecto) do
    query =
      selecto
      |> Selecto.select(["id", "name"])
      |> Selecto.filter({"id", {:lte, 3}})
      |> Selecto.order_by(["id"])

    assert {:ok, {rows, columns, _aliases}} = Selecto.execute(query)
    assert columns == ["id", "name"]
    assert rows == [[1, "item 1"], [2, "item 2"], [3, "item 3"]]
  end

  defp domain(table) do
    %{
      name: "Pool configure",
      source: %{
        source_table: table,
        primary_key: :id,
        fields: [:id, :name],
        redact_fields: [],
        columns: %{id: %{type: :integer}, name: %{type: :string}},
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end
end
