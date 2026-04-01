defmodule SelectoDBPostgreSQL.AdapterTest do
  use ExUnit.Case, async: true

  defmodule Pg18MockRepo do
    def query("show server_version_num", []) do
      {:ok, %{rows: [["180001"]]}}
    end
  end

  test "adapter exposes the selecto adapter contract" do
    assert Code.ensure_loaded?(SelectoDBPostgreSQL.Adapter)
    assert Code.ensure_loaded?(SelectoDBPostgreSQL.UpdatoAdapter)
    assert function_exported?(SelectoDBPostgreSQL.Adapter, :name, 0)
    assert function_exported?(SelectoDBPostgreSQL.Adapter, :connect, 1)
    assert function_exported?(SelectoDBPostgreSQL.Adapter, :execute, 4)
    assert function_exported?(SelectoDBPostgreSQL.Adapter, :placeholder, 1)
    assert function_exported?(SelectoDBPostgreSQL.Adapter, :quote_identifier, 1)
    assert function_exported?(SelectoDBPostgreSQL.Adapter, :supports?, 1)
    assert function_exported?(SelectoDBPostgreSQL.UpdatoAdapter, :merge_upsert_opts, 4)
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

  test "postgres adapter reports schema introspection support" do
    assert SelectoDBPostgreSQL.Adapter.supports?(:schema_introspection)
  end

  test "postgres adapter lists tables through schema introspection" do
    connection = %{
      query_fun: fn query, params, _opts ->
        assert query =~ "FROM information_schema.tables"
        assert params == ["public"]

        {:ok, %{rows: [["products"], ["users"]], columns: ["table_name"]}}
      end
    }

    assert {:ok, ["products", "users"]} =
             SelectoDBPostgreSQL.Adapter.list_tables(connection, schema: "public")
  end

  test "postgres adapter introspects table metadata and belongs_to associations" do
    connection = %{query_fun: &introspection_query_stub/3}

    assert {:ok, metadata} =
             SelectoDBPostgreSQL.Adapter.introspect_table(connection, "products",
               schema: "public"
             )

    assert metadata.table_name == "products"
    assert metadata.schema == "public"
    assert metadata.primary_key == :id
    assert metadata.fields == [:id, :name, :price, :category_id]
    assert metadata.field_types[:name] == :string
    assert metadata.field_types[:price] == :decimal
    assert metadata.field_types[:id] == :integer
    assert metadata.columns[:price].precision == 10
    assert metadata.columns[:name].nullable == false

    assert metadata.associations == %{
             category: %{
               association_type: :belongs_to,
               constraint_name: "products_category_id_fkey",
               field: :category,
               is_through: false,
               join_type: :inner,
               owner_key: :category_id,
               queryable: :categories,
               related_key: :id,
               related_module_name: "Category",
               related_schema: "Category",
               related_table: "categories",
               type: :belongs_to
             }
           }
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

  test "named atom connections are treated as postgrex connections, not repos" do
    assert :ok = SelectoDBPostgreSQL.Adapter.validate_connection(:named_postgrex_conn)

    assert %{type: :postgrex, pid: :named_postgrex_conn, status: :connected} =
             SelectoDBPostgreSQL.Adapter.connection_info(:named_postgrex_conn)
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

  defp introspection_query_stub(query, params, _opts) do
    cond do
      String.contains?(query, "FROM information_schema.columns") ->
        assert params == ["public", "products"]

        {:ok,
         %{
           rows: [
             ["id", "integer", "int4", "NO", nil, nil, 32, 0, 1],
             ["name", "character varying", "varchar", "NO", nil, 255, nil, nil, 2],
             ["price", "numeric", "numeric", "YES", nil, nil, 10, 2, 3],
             ["category_id", "integer", "int4", "YES", nil, nil, 32, 0, 4]
           ],
           columns: []
         }}

      String.contains?(query, "AND i.indisprimary") ->
        assert params == ["public", "products"]
        {:ok, %{rows: [["id"]], columns: ["attname"]}}

      String.contains?(query, "AND tc.table_name = $2") ->
        assert params == ["public", "products"]

        {:ok,
         %{rows: [["products_category_id_fkey", "category_id", "public", "categories", "id"]]}}

      true ->
        flunk("unexpected introspection query: #{query} with #{inspect(params)}")
    end
  end
end
