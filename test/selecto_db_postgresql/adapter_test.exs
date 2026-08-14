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

  test "connection URLs are rejected cleanly instead of reaching Postgrex as unsupported options" do
    url = "postgres://user:secret@example.test/database"

    assert {:error, {:invalid_connection_options, :expected_options_or_connection}} =
             SelectoDBPostgreSQL.Adapter.connect(url)

    assert {:error, {:invalid_connection_options, :url_option_not_supported}} =
             SelectoDBPostgreSQL.Adapter.connect(url: url)

    assert {:error, {:invalid_connection_options, :url_option_not_supported}} =
             SelectoDBPostgreSQL.Adapter.connect(%{url: url})

    assert {:error, {:invalid_connection_options, :expected_keyword_list}} =
             SelectoDBPostgreSQL.Adapter.connect([:not_a_keyword])
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

  test "postgres adapter reports materialized view refresh support" do
    assert SelectoDBPostgreSQL.Adapter.supports?(:materialized_view_refresh)
    assert SelectoDBPostgreSQL.Adapter.supports?(:materialized_view_refresh_concurrently)
  end

  test "write capabilities report version-dependent MERGE semantics" do
    pg18 = SelectoDBPostgreSQL.Adapter.write_capabilities(Pg18MockRepo)

    assert pg18.write_graph
    assert pg18.merge
    assert pg18.merge_returning
    assert pg18.merge_delete_missing
    assert pg18.server_major == 18

    unknown = SelectoDBPostgreSQL.Adapter.write_capabilities(:unused)
    assert unknown.write_graph
    refute unknown.merge
    refute unknown.merge_returning
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

  test "postgres adapter lists relations including views when requested" do
    connection = %{
      query_fun: fn query, params, _opts ->
        assert query =~ "pg_matviews"
        assert params == ["public"]

        {:ok,
         %{
           rows: [
             ["products", "table"],
             ["active_customers", "view"],
             ["daily_rollup", "materialized_view"]
           ]
         }}
      end
    }

    assert {:ok,
            [
              %{name: "products", source_kind: :table},
              %{name: "active_customers", source_kind: :view},
              %{name: "daily_rollup", source_kind: :materialized_view}
            ]} =
             SelectoDBPostgreSQL.Adapter.list_relations(connection,
               schema: "public",
               include_views: true
             )
  end

  test "postgres adapter refreshes materialized views" do
    connection = %{
      query_fun: fn query, params, opts ->
        assert query ==
                 ~s(REFRESH MATERIALIZED VIEW CONCURRENTLY "reporting"."daily_rollup";)

        assert params == []
        assert opts == [prepared: false]

        {:ok, %{rows: [], columns: []}}
      end
    }

    assert {:ok, _} =
             SelectoDBPostgreSQL.Adapter.refresh_materialized_view(
               connection,
               "reporting.daily_rollup",
               concurrently: true
             )
  end

  test "materialized-view refresh quotes safe names and rejects SQL-shaped names before dispatch" do
    parent = self()

    connection = %{
      query_fun: fn query, params, opts ->
        send(parent, {:dispatched, query, params, opts})
        {:ok, %{rows: [], columns: []}}
      end
    }

    assert {:ok, _} =
             SelectoDBPostgreSQL.Adapter.refresh_materialized_view(
               connection,
               "reporting.daily_rollup"
             )

    assert_receive {:dispatched, ~s(REFRESH MATERIALIZED VIEW "reporting"."daily_rollup";), [],
                    [prepared: false]}

    malicious = "reporting.safe; DROP TABLE accounts; --"

    assert {:error,
            %{
              code: :invalid_sql_identifier,
              identifier: ^malicious,
              reason: :invalid_characters
            }} =
             SelectoDBPostgreSQL.Adapter.refresh_materialized_view(connection, malicious)

    refute_receive {:dispatched, _query, _params, _opts}
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

  test "expanded introspection propagates association query failures" do
    query_fun = fn query, _params, _opts ->
      cond do
        String.contains?(query, "FROM information_schema.columns") ->
          {:ok, %{rows: [["id", "integer", "int4", "NO", nil, nil, 32, 0, 1]]}}

        String.contains?(query, "AND i.indisprimary") ->
          {:ok, %{rows: [["id"]]}}

        String.contains?(query, "AND tc.table_name = $2") ->
          {:ok, %{rows: []}}

        String.contains?(query, "AND ccu.table_name = $2") ->
          {:error, :permission_denied}
      end
    end

    assert {:error, {:reverse_foreign_keys_query_failed, :permission_denied}} =
             SelectoDBPostgreSQL.Adapter.introspect_table(%{query_fun: query_fun}, "products",
               schema: "public",
               expand: true
             )
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

  test "dead pool execution fails closed instead of exiting with noproc" do
    pool_pid = spawn(fn -> :ok end)
    monitor = Process.monitor(pool_pid)
    assert_receive {:DOWN, ^monitor, :process, ^pool_pid, _reason}

    pool_ref = %{adapter: SelectoDBPostgreSQL.Adapter, pool: pool_pid}

    assert {:error,
            %Selecto.Error{
              type: :connection_error,
              message: "PostgreSQL connection pool is not available",
              details: %{reason: :pool_process_not_alive}
            }} =
             SelectoDBPostgreSQL.Adapter.execute({:pool, pool_ref}, "select 1", [], [])
  end

  test "postgres adapter validates invalid connection info" do
    assert {:error, "Invalid connection configuration"} =
             SelectoDBPostgreSQL.Adapter.validate_connection(123)

    assert %{type: :unknown, status: :invalid} =
             SelectoDBPostgreSQL.Adapter.connection_info(123)
  end

  test "arbitrary registered processes are not treated as named Postgrex connections" do
    process = spawn(fn -> Process.sleep(:infinity) end)
    Process.register(process, :named_postgrex_conn)
    on_exit(fn -> if Process.alive?(process), do: Process.exit(process, :kill) end)

    assert {:error, {:invalid_connection, :named_postgrex_conn}} =
             SelectoDBPostgreSQL.Adapter.connect(:named_postgrex_conn)

    assert {:error, "Named Postgrex connection is not registered"} =
             SelectoDBPostgreSQL.Adapter.validate_connection(:named_postgrex_conn)

    assert %{type: :postgrex, pid: :named_postgrex_conn, status: :disconnected} =
             SelectoDBPostgreSQL.Adapter.connection_info(:named_postgrex_conn)
  end

  test "unregistered named connections fail closed before Postgrex dispatch" do
    name = :selecto_unregistered_postgrex_connection

    assert {:error, {:invalid_connection, ^name}} =
             SelectoDBPostgreSQL.Adapter.connect(name)

    assert {:error, "Named Postgrex connection is not registered"} =
             SelectoDBPostgreSQL.Adapter.validate_connection(name)

    assert %{type: :postgrex, pid: ^name, status: :disconnected} =
             SelectoDBPostgreSQL.Adapter.connection_info(name)

    assert {:error, {:invalid_connection, ^name}} =
             SelectoDBPostgreSQL.Adapter.execute(name, "select 1", [], [])

    assert {:error, {:invalid_connection, ^name}} =
             SelectoDBPostgreSQL.Adapter.stream(name, "select 1", [], [])
  end

  test "dead process connections fail closed instead of exiting with noproc" do
    process =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    monitor = Process.monitor(process)
    Process.exit(process, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^process, :killed}

    assert {:error, {:invalid_connection, ^process}} =
             SelectoDBPostgreSQL.Adapter.connect(process)

    assert {:error, "Postgrex connection process is not alive"} =
             SelectoDBPostgreSQL.Adapter.validate_connection(process)

    assert {:error, {:invalid_connection, ^process}} =
             SelectoDBPostgreSQL.Adapter.execute(process, "select 1", [], [])

    assert {:error, {:invalid_connection, ^process}} =
             SelectoDBPostgreSQL.Adapter.stream(process, "select 1", [], [])

    assert {:error, {:invalid_stream_pool, %{stream_context: :pool}}} =
             SelectoDBPostgreSQL.Adapter.stream({:pool, process}, "select 1", [], [])

    command = %Selecto.Write.Command{
      operation: :insert,
      relation: :items,
      assignments: [%{field: :name, value: {:literal, "item"}}]
    }

    assert {:error,
            %Selecto.Write.Error{
              type: :transaction_failed,
              details: %{adapter: :postgresql, reason: :invalid_connection}
            } = error} = SelectoDBPostgreSQL.Adapter.execute_write(process, command)

    refute inspect(error) =~ inspect(process)
  end

  test "stream producer crashes surface promptly instead of timing out" do
    {:ok, stream} =
      SelectoDBPostgreSQL.Adapter.stream(self(), "select 1", [],
        receive_timeout: 5_000,
        stream_producer: fn _send_chunk -> exit(:producer_crashed) end
      )

    started_at = System.monotonic_time(:millisecond)

    assert_raise RuntimeError,
                 ~r/PostgreSQL stream producer failed: \{:exit, :producer_crashed\}/,
                 fn ->
                   Enum.to_list(stream)
                 end

    assert System.monotonic_time(:millisecond) - started_at < 1_000
  end

  test "stream delivers rows to the process that enumerates it" do
    {:ok, stream} =
      SelectoDBPostgreSQL.Adapter.stream(self(), "select 1", [],
        receive_timeout: 500,
        stream_producer: fn send_chunk ->
          send_chunk.([[1, "one"], [2, "two"]], ["id", "name"])
          {:ok, :complete}
        end
      )

    enumerator = Task.async(fn -> Enum.to_list(stream) end)

    assert Task.await(enumerator, 2_000) == [
             {[1, "one"], ["id", "name"]},
             {[2, "two"], ["id", "name"]}
           ]
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
