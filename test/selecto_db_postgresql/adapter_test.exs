defmodule SelectoDBPostgreSQL.AdapterTest do
  use ExUnit.Case, async: true

  defmodule Pg18MockRepo do
    def query("show server_version_num", []) do
      {:ok, %{rows: [["180001"]]}}
    end
  end

  test "adapter exposes the selecto adapter contract" do
    assert Code.ensure_loaded?(SelectoDBPostgreSQL.Adapter)
    assert function_exported?(SelectoDBPostgreSQL.Adapter, :name, 0)
    assert function_exported?(SelectoDBPostgreSQL.Adapter, :connect, 1)
    assert function_exported?(SelectoDBPostgreSQL.Adapter, :execute, 4)
    assert function_exported?(SelectoDBPostgreSQL.Adapter, :placeholder, 1)
    assert function_exported?(SelectoDBPostgreSQL.Adapter, :quote_identifier, 1)
    assert function_exported?(SelectoDBPostgreSQL.Adapter, :supports?, 1)
  end

  test "postgres adapter reports expected placeholder and quoting strategy" do
    assert SelectoDBPostgreSQL.Adapter.placeholder(3) |> IO.iodata_to_binary() == "$3"
    assert SelectoDBPostgreSQL.Adapter.quote_identifier("order") == "\"order\""
  end

  test "postgres adapter rejects invalid connection values" do
    assert SelectoDBPostgreSQL.Adapter.execute(123, "select 1", [], []) ==
             {:error, {:invalid_connection, 123}}
  end

  test "postgres adapter supports pool references" do
    assert SelectoDBPostgreSQL.Adapter.connect({:pool, %{name: :demo}}) ==
             {:ok, {:pool, %{name: :demo}}}
  end

  test "postgres adapter reports stream support" do
    assert SelectoDBPostgreSQL.Adapter.supports?(:stream)
  end

  test "postgres adapter reports rollup support" do
    assert SelectoDBPostgreSQL.Adapter.supports?(:rollup)
  end

  test "postgres rollup uses compatibility wrapper by default" do
    selecto =
      sales_domain()
      |> Selecto.configure(:mock_connection,
        adapter: SelectoDBPostgreSQL.Adapter,
        validate: false
      )
      |> Selecto.select([{:sum, "amount"}])
      |> Selecto.group_by(rollup: ["region"])
      |> Selecto.order_by([{"region", :asc}])

    {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])

    assert String.contains?(sql, "rollup")
    assert String.contains?(sql, "select * from (")
    assert String.contains?(sql, ") as rollupfix")
  end

  test "postgres 18 disables rollup compatibility wrapper" do
    selecto =
      sales_domain()
      |> Selecto.configure(Pg18MockRepo, adapter: SelectoDBPostgreSQL.Adapter, validate: false)
      |> Selecto.select([{:sum, "amount"}])
      |> Selecto.group_by(rollup: ["region"])
      |> Selecto.order_by([{"region", :asc}])

    {sql, _aliases, _params} = Selecto.gen_sql(selecto, [])

    assert String.contains?(sql, "rollup")
    refute String.contains?(sql, "select * from (")
    refute String.contains?(sql, ") as rollupfix")
  end

  test "postgres adapter validates stream pool references" do
    assert {:error, {:invalid_stream_pool, %{stream_context: :pool}}} =
             SelectoDBPostgreSQL.Adapter.stream({:pool, %{}}, "select 1", [], [])
  end

  test "postgres adapter validates execute_pool references" do
    assert {:error, "Invalid pool reference"} =
             SelectoDBPostgreSQL.Adapter.execute_pool(:bad_ref, "select 1", [], [])
  end

  test "postgres adapter validates invalid connection info" do
    assert {:error, "Invalid connection configuration"} =
             SelectoDBPostgreSQL.Adapter.validate_connection(123)

    assert %{type: :unknown, status: :invalid} =
             SelectoDBPostgreSQL.Adapter.connection_info(123)
  end

  defp sales_domain do
    %{
      source: %{
        source_table: "sales",
        primary_key: :id,
        fields: [:id, :region, :amount],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          region: %{type: :string},
          amount: %{type: :decimal}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{},
      name: "Sales"
    }
  end
end
