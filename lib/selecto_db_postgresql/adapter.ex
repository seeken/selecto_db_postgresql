defmodule SelectoDBPostgreSQL.Adapter do
  @moduledoc """
  PostgreSQL adapter for Selecto.
  """

  @behaviour Selecto.DB.Adapter
  @behaviour Selecto.DB.WriteAdapter

  alias SelectoDBPostgreSQL.FunctionVerification
  alias SelectoDBPostgreSQL.Identifier
  alias SelectoDBPostgreSQL.GraphCompiler
  alias SelectoDBPostgreSQL.WriteCompiler
  alias Selecto.Write.{Batch, Command, CommittedEffectSink, Error, Graph, Result}
  alias Selecto.Write.Graph.Materializer

  @native_type_mappings %{
    "int2" => :integer,
    "int4" => :integer,
    "int8" => :integer,
    "float4" => :float,
    "float8" => :float,
    "numeric" => :decimal,
    "decimal" => :decimal,
    "money" => :decimal,
    "bpchar" => :string,
    "name" => :string,
    "citext" => :string,
    "bool" => :boolean,
    "timestamptz" => :utc_datetime,
    "jsonb" => :map,
    "_int4" => {:array, :integer},
    "_text" => {:array, :string},
    "_varchar" => {:array, :string},
    "_numeric" => {:array, :decimal},
    "inet" => :string,
    "cidr" => :string,
    "macaddr" => :string,
    "point" => :string,
    "line" => :string,
    "lseg" => :string,
    "box" => :string,
    "path" => :string,
    "polygon" => :string,
    "circle" => :string,
    "public.geometry" => :geometry,
    "public.geography" => :geography,
    "bytea" => :binary,
    "tsvector" => :text_search_document
  }

  @impl true
  def name, do: :postgresql

  @impl true
  def dialect, do: SelectoDBPostgreSQL.Dialect

  @impl true
  def identifier_policy, do: %{max_bytes: 63}

  @impl true
  def capability(:text_search) do
    %{
      feature: :text_search,
      supported?: true,
      modes: [:websearch, :plain, :phrase, :boolean, :natural, :prefix],
      governed_lookup?: true,
      default_mode: :websearch,
      document_type: :text_search_document,
      help: "Full-text search with web-style, plain, phrase, or boolean query modes."
    }
  end

  def capability(feature) do
    %{feature: feature, supported?: supports?(feature)}
  end

  @impl true
  def analyze_query(selecto, options),
    do: SelectoDBPostgreSQL.QueryAnalyzer.analyze_query(selecto, options)

  @impl true
  def analyze_index_usage(selecto, options),
    do: SelectoDBPostgreSQL.QueryAnalyzer.analyze_index_usage(selecto, options)

  @impl true
  def table_statistics(selecto, _options),
    do: SelectoDBPostgreSQL.QueryAnalyzer.get_table_statistics(selecto)

  @impl true
  def normalize_type(type) when is_binary(type) do
    normalized = type |> String.trim() |> String.downcase()
    Map.get(@native_type_mappings, normalized, type)
  end

  def normalize_type(:jsonb), do: :map
  def normalize_type(:tsvector), do: :text_search_document
  def normalize_type(:citext), do: :string
  def normalize_type(type), do: Selecto.TypeSystem.normalize_type(type)

  @impl true
  def type_family(type), do: type |> normalize_type() |> Selecto.TypeFamily.of()

  @impl true
  def normalize_execution_result(result), do: {:ok, normalize_result(result)}

  @impl true
  def normalize_error(%Selecto.Error{} = error), do: error

  def normalize_error(%Postgrex.Error{} = error) do
    native = Map.get(error, :postgres) || %{}
    category = normalize_error_category(Map.get(native, :code) || Map.get(native, :pg_code))

    Selecto.Error.query_error(Exception.message(error), nil, [], %{
      category: category,
      constraint: Map.get(native, :constraint),
      column: Map.get(native, :column),
      recoverable?: category in [:unique_violation, :foreign_key_violation, :not_null_violation]
    })
  end

  def normalize_error(reason), do: Selecto.Error.from_reason(reason)

  @impl true
  def connect({:pool, _} = pool_ref), do: {:ok, pool_ref}

  def connect(connection) when is_pid(connection) do
    if Process.alive?(connection),
      do: {:ok, connection},
      else: {:error, {:invalid_connection, connection}}
  end

  def connect(connection) when is_atom(connection) and not is_nil(connection) do
    if valid_named_connection?(connection),
      do: {:ok, connection},
      else: {:error, {:invalid_connection, connection}}
  end

  def connect(%DBConnection{} = connection), do: {:ok, connection}

  def connect(opts) when is_map(opts), do: connect(Map.to_list(opts))

  def connect(opts) when is_list(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, {:invalid_connection_options, :expected_keyword_list}}

      Keyword.has_key?(opts, :url) ->
        {:error, {:invalid_connection_options, :url_option_not_supported}}

      true ->
        connect_postgrex(opts)
    end
  end

  def connect(_other),
    do: {:error, {:invalid_connection_options, :expected_options_or_connection}}

  @impl true
  def disconnect(connection) when is_pid(connection) do
    if Process.alive?(connection), do: GenServer.stop(connection)
    :ok
  end

  def disconnect(_connection), do: :ok

  @impl true
  def execute({:pool, pool_ref}, query, params, opts) do
    case Selecto.ConnectionPool.execute(pool_ref, normalize_query(query), params, opts) do
      {:ok, result} -> {:ok, normalize_result(result)}
      {:error, reason} -> {:error, reason}
    end
  end

  def execute(connection, query, params, opts) when is_atom(connection) do
    if valid_named_connection?(connection) do
      execute_module_connection(connection, query, params, opts)
    else
      {:error, {:invalid_connection, connection}}
    end
  end

  def execute(connection, query, params, opts) when is_pid(connection) do
    if Process.alive?(connection),
      do: query_postgrex(connection, query, params, opts),
      else: {:error, {:invalid_connection, connection}}
  end

  def execute(%DBConnection{} = connection, query, params, opts) do
    query_postgrex(connection, query, params, opts)
  end

  def execute(connection, _query, _params, _opts), do: {:error, {:invalid_connection, connection}}

  @impl Selecto.DB.WriteAdapter
  def write_capabilities(connection) do
    server_major =
      case server_version_major(connection) do
        {:ok, major} -> major
        _ -> nil
      end

    %{
      protocol_version: Selecto.Write.Capabilities.protocol_version(),
      insert: true,
      update: true,
      upsert: true,
      delete: true,
      write_graph: true,
      returning: true,
      generated_keys: :returning,
      atomic_batch: true,
      transactions: true,
      committed_effect_sink: true,
      dialect: :postgresql,
      server_major: server_major,
      merge: is_integer(server_major) and server_major >= 15,
      merge_returning: is_integer(server_major) and server_major >= 17,
      merge_delete_missing: is_integer(server_major) and server_major >= 17
    }
  end

  @impl Selecto.DB.WriteAdapter
  def preview_write(connection, command, opts \\ [])

  def preview_write(connection, %Graph{} = graph, opts) do
    with :ok <- validate_write_graph(graph) do
      GraphCompiler.preview(graph, graph_server_major(connection, opts), opts)
    end
  end

  def preview_write(_connection, %Batch{} = batch, opts) do
    with :ok <- validate_write_batch(batch) do
      WriteCompiler.preview(batch, opts)
    end
  end

  def preview_write(_connection, %Command{} = command, opts) do
    with :ok <- validate_write_command(command) do
      WriteCompiler.preview(command, opts)
    end
  end

  def preview_write(_connection, command, _opts), do: invalid_write_input(command)

  @impl Selecto.DB.WriteAdapter
  def execute_write(connection, command, opts \\ [])

  def execute_write(connection, %Command{} = command, opts) do
    with :ok <- validate_write_command(command) do
      with_postgres_transaction(connection, opts, fn transactional_connection ->
        with {:ok, result} <- execute_write_command(transactional_connection, command, opts),
             :ok <- invoke_committed_effect_sink(transactional_connection, result, command, opts) do
          {:ok, result}
        end
      end)
    end
  end

  def execute_write(connection, %Batch{} = batch, opts) do
    with :ok <- validate_write_batch(batch) do
      with_postgres_transaction(connection, opts, fn transactional_connection ->
        batch.commands
        |> Enum.reduce_while({:ok, []}, fn command, {:ok, results} ->
          case execute_write_command(transactional_connection, command, opts) do
            {:ok, result} -> {:cont, {:ok, [result | results]}}
            {:error, %Error{} = error} -> {:halt, {:error, error}}
          end
        end)
        |> case do
          {:ok, results} ->
            results = Enum.reverse(results)

            with :ok <-
                   invoke_committed_effect_sink(
                     transactional_connection,
                     results,
                     batch,
                     opts
                   ) do
              {:ok, results}
            end

          {:error, error} ->
            {:error, error}
        end
      end)
    end
  end

  def execute_write(connection, %Graph{} = graph, opts) do
    with :ok <- validate_write_graph(graph) do
      server_major = graph_server_major(connection, opts)

      with_postgres_transaction(connection, opts, fn transactional_connection ->
        with {:ok, result} <- execute_graph(transactional_connection, graph, server_major, opts),
             :ok <- invoke_committed_effect_sink(transactional_connection, result, graph, opts) do
          {:ok, result}
        end
      end)
    end
  end

  def execute_write(_connection, command, _opts), do: invalid_write_input(command)

  defp validate_write_batch(%Batch{} = batch), do: Batch.validate(batch)
  defp validate_write_graph(%Graph{} = graph), do: Graph.validate(graph)
  defp validate_write_command(%Command{} = command), do: Command.validate(command)

  defp invalid_write_input(command) do
    {:error,
     Error.new(:invalid_command, "expected a portable write command, batch, or graph",
       details: %{actual: command}
     )}
  end

  defp invoke_committed_effect_sink(connection, result, write, opts) do
    CommittedEffectSink.invoke(
      Keyword.get(opts, :committed_effect_sink),
      connection,
      result,
      %{adapter: :postgresql, write: write}
    )
  end

  defp execute_graph(connection, %Graph{} = graph, server_major, opts) do
    graph.nodes
    |> Enum.reduce_while({:ok, %{}, 0, []}, fn node, {:ok, results, affected_rows, strategies} ->
      with {:ok, materialized} <- Materializer.materialize_node(node, results),
           {:ok, node_results, node_affected, strategy} <-
             execute_graph_node(connection, materialized, server_major, opts) do
        results =
          Map.merge(
            results,
            Map.new(node_results, fn {row_id, result} -> {{node.id, row_id}, result} end)
          )

        {:cont,
         {:ok, results, affected_rows + node_affected, strategies ++ [{node.id, strategy}]}}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, results, affected_rows, strategies} ->
        {:ok,
         %Result{
           operation: :graph,
           affected_rows: affected_rows,
           rows: Materializer.root_rows(graph, results),
           metadata:
             %{
               dialect: :postgresql,
               atomic?: true,
               server_major: server_major,
               node_strategies: Map.new(strategies)
             }
             |> Map.merge(Materializer.outcome_metadata(graph, results))
         }}

      error ->
        error
    end
  end

  defp execute_graph_node(connection, node, server_major, opts) do
    if GraphCompiler.merge_eligible?(node, server_major) do
      with {:ok, statement} <- GraphCompiler.compile_merge(node, opts),
           {:ok, query_result} <- execute(connection, statement.text, statement.params, opts),
           {:ok, {results, affected_rows}} <- GraphCompiler.merge_results(node, query_result) do
        {:ok, results, affected_rows, :merge}
      else
        {:error, %Error{} = error} -> {:error, error}
        {:error, reason} -> {:error, write_error(:execution_failed, reason)}
      end
    else
      execute_graph_node_fallback(connection, node, opts)
    end
  end

  defp execute_graph_node_fallback(connection, node, opts) do
    with {:ok, row_results, affected_rows} <- execute_graph_rows(connection, node.rows, opts),
         {:ok, cleanup} <- Materializer.delete_missing_command(node, row_results),
         {:ok, cleanup_affected} <- execute_graph_cleanup(connection, cleanup, opts) do
      {:ok, row_results, affected_rows + cleanup_affected, :ordered_fallback}
    end
  end

  defp execute_graph_rows(connection, rows, opts) do
    Enum.reduce_while(rows, {:ok, %{}, 0}, fn row, {:ok, results, affected_rows} ->
      case execute_write_command(connection, row.command, opts) do
        {:ok, result} ->
          {:cont, {:ok, Map.put(results, row.id, result), affected_rows + result.affected_rows}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end

  defp execute_graph_cleanup(_connection, nil, _opts), do: {:ok, 0}

  defp execute_graph_cleanup(connection, command, opts) do
    case execute_write_command(connection, command, opts) do
      {:ok, result} -> {:ok, result.affected_rows}
      {:error, _} = error -> error
    end
  end

  defp graph_server_major(connection, opts) do
    case Keyword.fetch(opts, :server_version_major) do
      {:ok, major} when is_integer(major) and major > 0 ->
        major

      _ ->
        case server_version_major(connection) do
          {:ok, major} -> major
          _ -> 0
        end
    end
  end

  defp execute_write_command(connection, %Command{} = command, opts) do
    with {:ok, statement} <- WriteCompiler.compile(command, opts),
         {:ok, query_result} <- execute(connection, statement.text, statement.params, opts),
         {:ok, affected_rows} <- enforce_cardinality(command, query_result) do
      {:ok,
       %Result{
         operation: command.operation,
         affected_rows: affected_rows,
         rows: result_rows(query_result),
         metadata: %{dialect: :postgresql}
       }}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, write_error(:execution_failed, reason)}
    end
  end

  @impl true
  def execute_pool(pool_ref, query, params, opts) do
    query = normalize_query(query)
    use_prepared = Keyword.get(opts, :prepared, true)
    cache_key = if use_prepared, do: Selecto.ConnectionPool.generate_cache_key(query), else: nil

    case resolve_live_pool_pid(pool_ref) do
      {:ok, pool_pid} ->
        execute_with_pool_pid(pool_pid, query, params, cache_key, opts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def execute_raw(connection, query, params) do
    cond do
      match?({:pool, _}, connection) ->
        case execute(connection, query, params, prepared: false) do
          {:ok, result} -> {:ok, result}
          {:error, reason} -> {:error, Selecto.Error.from_reason(reason)}
        end

      is_atom(connection) and ecto_repo?(connection) ->
        execute_ecto_query(connection, query, params)

      valid_postgrex_connection?(connection) ->
        case execute(connection, query, params, []) do
          {:ok, result} -> {:ok, result}
          {:error, reason} -> {:error, Selecto.Error.from_reason(reason)}
        end

      true ->
        {:error,
         Selecto.Error.connection_error("Invalid connection type", %{
           connection: inspect(connection)
         })}
    end
  rescue
    e ->
      {:error, Selecto.Error.from_reason(e)}
  end

  @impl true
  def placeholder(index), do: ["$", Integer.to_string(index)]

  @impl true
  def quote_identifier(identifier) when is_binary(identifier) do
    escaped = String.replace(identifier, "\"", "\"\"")
    "\"#{escaped}\""
  end

  def quote_identifier(identifier), do: identifier |> to_string() |> quote_identifier()

  @impl true
  def format_datetime(expression, format) when is_binary(format) do
    escaped = String.replace(format, "'", "''")
    ["to_char(", expression, ", '", escaped, "')"]
  end

  @impl true
  def rollup_sql(grouped_clauses), do: ["rollup( ", grouped_clauses, " )"]

  @impl true
  def supports?(feature) do
    feature in [
      :cte,
      :jsonb,
      :array_ops,
      :array_any_comparison,
      :native_null_ordering,
      :rollup,
      :returning,
      :text_search,
      :window_functions,
      :lateral_join,
      :prefix,
      :stream,
      :function_verification,
      :schema_introspection,
      :materialized_view_refresh,
      :materialized_view_refresh_concurrently
    ]
  end

  @impl true
  def verify_function(connection, %Selecto.FunctionVerification.Request{} = request, opts) do
    FunctionVerification.verify(
      request,
      opts,
      fn query, params -> introspection_query(connection, query, params) end,
      fn statement, probe_opts -> prepare_function_probe(connection, statement, probe_opts) end
    )
  end

  @impl true
  def refresh_materialized_view(connection, database_name, opts \\ []) do
    with {:ok, quoted_name} <-
           Selecto.SQL.QualifiedIdentifier.quote(database_name, __MODULE__) do
      concurrently = if Keyword.get(opts, :concurrently, false), do: " CONCURRENTLY", else: ""
      query = "REFRESH MATERIALIZED VIEW#{concurrently} #{quoted_name};"

      case introspection_query(connection, query, []) do
        {:ok, result} -> {:ok, result}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_tables(connection, opts \\ []) do
    schema = Keyword.get(opts, :schema, "public")

    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = $1
      AND table_type = 'BASE TABLE'
    ORDER BY table_name
    """

    case introspection_query(connection, query, [schema]) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [table_name] -> table_name end)}
      {:error, reason} -> {:error, {:query_failed, reason}}
    end
  end

  @impl true
  def list_relations(connection, opts \\ []) do
    schema = Keyword.get(opts, :schema, "public")
    include_views = Keyword.get(opts, :include_views, false)

    query =
      if include_views do
        """
        SELECT table_name,
               CASE table_type
                 WHEN 'BASE TABLE' THEN 'table'
                 WHEN 'VIEW' THEN 'view'
               END AS source_kind
        FROM information_schema.tables
        WHERE table_schema = $1
          AND table_type IN ('BASE TABLE', 'VIEW')
        UNION ALL
        SELECT matviewname AS table_name,
               'materialized_view' AS source_kind
        FROM pg_matviews
        WHERE schemaname = $1
        ORDER BY table_name
        """
      else
        """
        SELECT table_name, 'table' AS source_kind
        FROM information_schema.tables
        WHERE table_schema = $1
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
      end

    case introspection_query(connection, query, [schema]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [table_name, source_kind] ->
           %{name: table_name, source_kind: normalize_relation_source_kind(source_kind)}
         end)}

      {:error, reason} ->
        {:error, {:query_failed, reason}}
    end
  end

  @impl true
  def introspect_table(connection, table_name, opts \\ []) do
    schema = Keyword.get(opts, :schema, "public")
    include_associations = Keyword.get(opts, :include_associations, true)
    expand = Keyword.get(opts, :expand, false)

    with {:ok, columns} <- get_columns(connection, table_name, schema),
         {:ok, primary_key} <- get_primary_key(connection, table_name, schema),
         {:ok, foreign_keys} <- get_foreign_keys(connection, table_name, schema) do
      fields = Enum.map(columns, & &1.column_name)

      field_types =
        Enum.into(columns, %{}, fn column ->
          {column.column_name, map_pg_type(connection, column.data_type, column.udt_name)}
        end)

      with {:ok, associations} <-
             table_associations(
               connection,
               table_name,
               schema,
               primary_key,
               foreign_keys,
               include_associations,
               expand
             ) do
        column_metadata =
          Enum.into(columns, %{}, fn column ->
            {column.column_name,
             %{
               type: Map.get(field_types, column.column_name),
               nullable: column.is_nullable == "YES",
               default: column.column_default,
               max_length: column.character_maximum_length,
               precision: column.numeric_precision,
               scale: column.numeric_scale
             }}
          end)

        {:ok,
         %{
           table_name: table_name,
           schema: schema,
           fields: fields,
           field_types: field_types,
           primary_key: primary_key,
           associations: associations,
           columns: column_metadata,
           source: :postgresql
         }}
      end
    end
  end

  defp table_associations(
         _connection,
         _table_name,
         _schema,
         _primary_key,
         _foreign_keys,
         false,
         _expand
       ),
       do: {:ok, %{}}

  defp table_associations(
         connection,
         table_name,
         schema,
         primary_key,
         _foreign_keys,
         true,
         true
       ) do
    build_expanded_associations(connection, table_name, schema, primary_key)
  end

  defp table_associations(
         _connection,
         _table_name,
         _schema,
         _primary_key,
         foreign_keys,
         true,
         false
       ),
       do: {:ok, build_associations(foreign_keys)}

  @impl true
  def rollup_literal_order(index), do: [Integer.to_string(index), " asc nulls first"]

  @impl true
  def rollup_sort_fix(connection) do
    case server_version_major(connection) do
      {:ok, major} when is_integer(major) and major >= 18 -> false
      _ -> true
    end
  end

  @impl true
  def stream({:pool, pool_ref}, query, params, opts) do
    case resolve_stream_pool_connection(pool_ref) do
      {:ok, pool_conn} -> {:ok, build_postgrex_cursor_stream(pool_conn, query, params, opts)}
      {:error, details} -> {:error, {:invalid_stream_pool, details}}
    end
  end

  def stream(conn, query, params, opts) when is_pid(conn) do
    if Process.alive?(conn),
      do: {:ok, build_postgrex_cursor_stream(conn, query, params, opts)},
      else: {:error, {:invalid_connection, conn}}
  end

  def stream(conn, query, params, opts) when is_atom(conn) and not is_nil(conn) do
    if valid_named_connection?(conn),
      do: {:ok, build_postgrex_cursor_stream(conn, query, params, opts)},
      else: {:error, {:invalid_connection, conn}}
  end

  def stream(connection, _query, _params, _opts) do
    {:error, {:invalid_connection, connection}}
  end

  @server_version_num_query "show server_version_num"

  @impl true
  def server_version_major(connection) do
    case fetch_server_version_num(connection) do
      {:ok, version_num} -> {:ok, div(version_num, 10_000)}
      {:error, _reason} = error -> error
    end
  end

  @impl true
  def validate_connection(connection) do
    cond do
      is_atom(connection) and not is_nil(connection) ->
        if valid_named_connection?(connection),
          do: :ok,
          else: {:error, "Named Postgrex connection is not registered"}

      match?({:pool, _}, connection) ->
        validate_pool_connection(connection)

      is_pid(connection) ->
        if Process.alive?(connection),
          do: :ok,
          else: {:error, "Postgrex connection process is not alive"}

      true ->
        {:error, "Invalid connection configuration"}
    end
  end

  @impl true
  def connection_info(connection) do
    cond do
      is_atom(connection) and not is_nil(connection) ->
        %{
          type: :postgrex,
          pid: connection,
          status: if(valid_named_connection?(connection), do: :connected, else: :disconnected)
        }

      match?({:pool, _}, connection) ->
        %{
          type: :connection_pool,
          pool_ref: elem(connection, 1),
          status: :connected,
          pool_stats: pool_stats(connection)
        }

      is_pid(connection) ->
        %{
          type: :postgrex,
          pid: connection,
          status: if(Process.alive?(connection), do: :connected, else: :disconnected)
        }

      true ->
        %{type: :unknown, value: connection, status: :invalid}
    end
  end

  @impl true
  def with_connection(pool_ref, fun) when is_function(fun, 1) do
    case resolve_live_pool_pid(pool_ref) do
      {:ok, pool_pid} ->
        try do
          result = fun.(pool_pid)
          {:ok, result}
        rescue
          e in DBConnection.ConnectionError ->
            {:error, Selecto.Error.connection_error(Exception.message(e), %{exception: e})}

          e ->
            {:error, Selecto.Error.query_error(Exception.message(e), nil, [], %{exception: e})}
        catch
          :exit, reason ->
            {:error,
             Selecto.Error.connection_error("PostgreSQL connection pool exited", %{
               exit_reason: reason
             })}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def transaction(connection, fun, opts \\ [])

  def transaction(%DBConnection{} = connection, fun, opts) when is_function(fun, 1) do
    run_postgrex_transaction(connection, fun, opts)
  end

  def transaction(connection, fun, opts)
      when is_function(fun, 1) and (is_pid(connection) or is_atom(connection)) do
    if not ecto_repo?(connection) and valid_postgrex_connection?(connection) do
      run_postgrex_transaction(connection, fun, opts)
    else
      transaction_from_managed_pool(connection, fun, opts)
    end
  end

  def transaction(pool_ref, fun, opts) when is_function(fun, 1) do
    transaction_from_managed_pool(pool_ref, fun, opts)
  end

  defp transaction_from_managed_pool(pool_ref, fun, opts) do
    case resolve_live_pool_pid(pool_ref) do
      {:ok, pool_pid} ->
        run_postgrex_transaction(pool_pid, fun, opts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp run_postgrex_transaction(connection, fun, opts) do
    Postgrex.transaction(connection, fun, opts)
  rescue
    e in DBConnection.ConnectionError ->
      {:error, Selecto.Error.connection_error(Exception.message(e), %{exception: e})}

    e in Postgrex.Error ->
      {:error, Selecto.Error.query_error(Exception.message(e), nil, [], %{exception: e})}

    e ->
      {:error, Selecto.Error.query_error(Exception.message(e), nil, [], %{exception: e})}
  catch
    :exit, reason ->
      {:error,
       Selecto.Error.connection_error("PostgreSQL connection pool exited", %{
         exit_reason: reason
       })}
  end

  defp normalize_relation_source_kind("table"), do: :table
  defp normalize_relation_source_kind("view"), do: :view
  defp normalize_relation_source_kind("materialized_view"), do: :materialized_view
  defp normalize_relation_source_kind(other), do: other

  @impl true
  def start_pool(connection_config, pool_config, pool_name) do
    case Selecto.ConnectionPool.get_manager_pid_by_name(pool_name) do
      {:ok, manager_pid} ->
        Selecto.ConnectionPool.build_pool_ref_from_manager(manager_pid)

      :error ->
        dbconnection_opts = [
          name: pool_name,
          pool: DBConnection.ConnectionPool,
          pool_size: pool_config[:pool_size],
          pool_overflow: pool_config[:max_overflow],
          timeout: pool_config[:connection_timeout],
          queue_target: pool_config[:checkout_timeout],
          queue_interval: 1000
        ]

        postgrex_opts = Keyword.merge(connection_config, dbconnection_opts)

        case start_postgrex_connection(postgrex_opts) do
          {:ok, pool_pid, started_new_pool?} ->
            manager_opts = [
              adapter: __MODULE__,
              pool_pid: pool_pid,
              pool_name: pool_name,
              pool_config: pool_config,
              connection_config: connection_config
            ]

            case Selecto.ConnectionPool.start_manager(manager_opts) do
              {:ok, manager_pid, :started} ->
                Selecto.ConnectionPool.build_pool_ref_from_manager(manager_pid)

              {:ok, manager_pid, :existing} ->
                if started_new_pool?, do: GenServer.stop(pool_pid)
                Selecto.ConnectionPool.build_pool_ref_from_manager(manager_pid)

              {:error, reason} ->
                if started_new_pool?, do: GenServer.stop(pool_pid)
                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  defp normalize_query(query) when is_binary(query), do: query
  defp normalize_query(query), do: IO.iodata_to_binary(query)

  defp resolve_live_pool_pid(pool_ref) do
    case Selecto.ConnectionPool.get_pool_pid(pool_ref) do
      {:ok, pool_pid} when is_pid(pool_pid) ->
        if Process.alive?(pool_pid) do
          {:ok, pool_pid}
        else
          {:error,
           Selecto.Error.connection_error("PostgreSQL connection pool is not available", %{
             reason: :pool_process_not_alive
           })}
        end

      {:ok, _pool_pid} ->
        {:error,
         Selecto.Error.connection_error("PostgreSQL connection pool is not available", %{
           reason: :invalid_pool_process
         })}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # An Ecto repository is a supervision tree, not a DBConnection process. Route it
  # through Ecto when the host application has Ecto available; keep it optional so
  # native Postgrex users do not need Ecto in their dependency graph.
  defp execute_module_connection(connection, query, params, opts) do
    if ecto_repo?(connection) do
      execute_ecto_query(connection, query, params)
    else
      query_postgrex(connection, query, params, opts)
    end
  end

  defp execute_ecto_query(repo, query, params) do
    case apply(ecto_sql_module(), :query, [repo, normalize_query(query), params]) do
      {:ok, result} -> {:ok, normalize_result(result)}
      {:error, reason} -> {:error, reason}
    end
  rescue
    exception -> {:error, {:query_exception, exception.__struct__, Exception.message(exception)}}
  catch
    :exit, reason -> {:error, {:connection_exit, reason}}
  end

  defp ecto_repo?(module) when is_atom(module) do
    module_exports?(module, :__adapter__, 0) and module_exports?(ecto_sql_module(), :query, 3)
  end

  defp ecto_repo?(_), do: false

  defp ecto_sql_module, do: :"Elixir.Ecto.Adapters.SQL"

  defp module_exports?(module, function, arity) do
    case :code.ensure_loaded(module) do
      {:module, ^module} -> function_exported?(module, function, arity)
      {:error, _reason} -> false
    end
  end

  defp valid_named_connection?(connection) when is_atom(connection) and not is_nil(connection) do
    ecto_repo?(connection) or module_exports?(connection, :query, 2) or
      named_postgrex_connection?(connection)
  end

  defp valid_named_connection?(_connection), do: false

  defp named_postgrex_connection?(connection) do
    with pid when is_pid(pid) <- Process.whereis(connection),
         {:dictionary, dictionary} <- Process.info(pid, :dictionary) do
      Keyword.get(dictionary, :connection_module) == Postgrex.Protocol
    else
      _other -> false
    end
  end

  defp valid_postgrex_connection?(connection) when is_pid(connection),
    do: Process.alive?(connection)

  defp valid_postgrex_connection?(connection), do: valid_named_connection?(connection)

  defp connect_postgrex(opts) do
    with {:ok, _started_apps} <- Application.ensure_all_started(:postgrex),
         {:ok, connection} <- Postgrex.start_link(opts) do
      {:ok, connection}
    end
  rescue
    exception ->
      {:error, {:invalid_connection_options, exception.__struct__, Exception.message(exception)}}
  catch
    :exit, reason -> {:error, {:connection_exit, reason}}
  end

  defp query_postgrex(connection, query, params, opts) do
    case Postgrex.query(connection, normalize_query(query), params, opts) do
      {:ok, result} -> {:ok, normalize_result(result)}
      {:error, reason} -> {:error, reason}
    end
  rescue
    exception -> {:error, {:query_exception, exception.__struct__, Exception.message(exception)}}
  catch
    :exit, reason -> {:error, {:connection_exit, reason}}
  end

  defp introspection_query(connection, query, params) do
    case connection do
      %{query_fun: query_fun} when is_function(query_fun, 3) ->
        query_fun.(query, params, prepared: false)

      _ ->
        execute(connection, query, params, prepared: false)
    end
  end

  defp prepare_function_probe(%{prepare_fun: prepare_fun}, statement, opts)
       when is_function(prepare_fun, 2) do
    prepare_fun.(statement, opts)
  end

  defp prepare_function_probe(connection, statement, opts) do
    with {:ok, target} <- function_probe_target(connection) do
      DBConnection.run(target, fn checked_out_connection ->
        case Postgrex.prepare(checked_out_connection, "", statement, opts) do
          {:ok, query} ->
            probe = %{
              columns: query.columns || [],
              result_oids: query.result_oids || []
            }

            case Postgrex.close(checked_out_connection, query, opts) do
              :ok -> {:ok, probe}
              {:error, reason} -> {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end
      end)
    end
  rescue
    exception -> {:error, {:probe_exception, exception.__struct__}}
  catch
    :exit, _reason -> {:error, :probe_connection_exit}
  end

  defp function_probe_target({:pool, pool_ref}), do: resolve_live_pool_pid(pool_ref)
  defp function_probe_target(%DBConnection{} = connection), do: {:ok, connection}

  defp function_probe_target(connection) when is_pid(connection) do
    if Process.alive?(connection),
      do: {:ok, connection},
      else: {:error, :invalid_probe_connection}
  end

  defp function_probe_target(connection) when is_atom(connection) and not is_nil(connection) do
    cond do
      ecto_repo?(connection) -> ecto_pool_target(connection)
      is_pid(Process.whereis(connection)) -> {:ok, connection}
      true -> {:error, :invalid_probe_connection}
    end
  end

  defp function_probe_target(_connection), do: {:error, :unsupported_probe_connection}

  defp ecto_pool_target(repo) do
    ecto_adapter = :"Elixir.Ecto.Adapter"

    if module_exports?(ecto_adapter, :lookup_meta, 1) do
      case apply(ecto_adapter, :lookup_meta, [repo]) do
        %{pid: pool_pid} when is_pid(pool_pid) -> {:ok, pool_pid}
        _metadata -> {:error, :unsupported_ecto_pool}
      end
    else
      {:error, :unsupported_ecto_pool}
    end
  end

  defp get_columns(connection, table_name, schema) do
    query = """
    SELECT
      column_name,
      data_type,
      udt_name,
      is_nullable,
      column_default,
      character_maximum_length,
      numeric_precision,
      numeric_scale,
      ordinal_position
    FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = $2
    ORDER BY ordinal_position
    """

    case introspection_query(connection, query, [schema, table_name]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [
                             column_name,
                             data_type,
                             udt_name,
                             is_nullable,
                             column_default,
                             max_length,
                             precision,
                             scale,
                             _ordinal_position
                           ] ->
           %{
             column_name: Identifier.to_atom!(column_name),
             data_type: data_type,
             udt_name: udt_name,
             is_nullable: is_nullable,
             column_default: column_default,
             character_maximum_length: max_length,
             numeric_precision: precision,
             numeric_scale: scale
           }
         end)}

      {:error, reason} ->
        {:error, {:columns_query_failed, reason}}
    end
  end

  defp get_primary_key(connection, table_name, schema) do
    query = """
    SELECT a.attname
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid
      AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = ($1 || '.' || $2)::regclass
      AND i.indisprimary
    ORDER BY a.attnum
    """

    case introspection_query(connection, query, [schema, table_name]) do
      {:ok, %{rows: []}} -> {:ok, nil}
      {:ok, %{rows: [[single_key]]}} -> {:ok, Identifier.to_atom!(single_key)}
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [key] -> Identifier.to_atom!(key) end)}
      {:error, reason} -> {:error, {:primary_key_query_failed, reason}}
    end
  end

  defp get_foreign_keys(connection, table_name, schema) do
    query = """
    SELECT
      tc.constraint_name,
      kcu.column_name,
      ccu.table_schema AS foreign_table_schema,
      ccu.table_name AS foreign_table_name,
      ccu.column_name AS foreign_column_name
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = $1
      AND tc.table_name = $2
    ORDER BY tc.constraint_name, kcu.ordinal_position
    """

    case introspection_query(connection, query, [schema, table_name]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [
                             constraint_name,
                             column_name,
                             foreign_schema,
                             foreign_table,
                             foreign_col
                           ] ->
           %{
             constraint_name: constraint_name,
             column_name: Identifier.to_atom!(column_name),
             foreign_table_schema: foreign_schema,
             foreign_table_name: foreign_table,
             foreign_column_name: Identifier.to_atom!(foreign_col)
           }
         end)}

      {:error, reason} ->
        {:error, {:foreign_keys_query_failed, reason}}
    end
  end

  defp get_reverse_foreign_keys(connection, table_name, schema) do
    query = """
    SELECT
      tc.table_name AS referencing_table,
      kcu.column_name AS referencing_column,
      ccu.column_name AS referenced_column,
      tc.constraint_name
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND ccu.table_schema = $1
      AND ccu.table_name = $2
    ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
    """

    case introspection_query(connection, query, [schema, table_name]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [
                             referencing_table,
                             referencing_column,
                             referenced_column,
                             constraint_name
                           ] ->
           %{
             referencing_table: referencing_table,
             referencing_column: Identifier.to_atom!(referencing_column),
             referenced_column: Identifier.to_atom!(referenced_column),
             constraint_name: constraint_name
           }
         end)}

      {:error, reason} ->
        {:error, {:reverse_foreign_keys_query_failed, reason}}
    end
  end

  defp build_associations(foreign_keys) do
    Enum.into(foreign_keys, %{}, fn foreign_key ->
      association_name =
        foreign_key.column_name
        |> Atom.to_string()
        |> String.replace_suffix("_id", "")
        |> Identifier.to_atom!()

      related_module_name = table_name_to_module(foreign_key.foreign_table_name)

      {association_name,
       %{
         type: :belongs_to,
         association_type: :belongs_to,
         related_schema: related_module_name,
         related_module_name: related_module_name,
         related_table: foreign_key.foreign_table_name,
         queryable: Identifier.to_atom!(foreign_key.foreign_table_name),
         field: association_name,
         owner_key: foreign_key.column_name,
         related_key: foreign_key.foreign_column_name,
         join_type: :inner,
         is_through: false,
         constraint_name: foreign_key.constraint_name
       }}
    end)
  end

  defp build_expanded_associations(connection, table_name, schema, primary_key) do
    with {:ok, foreign_keys} <- get_foreign_keys(connection, table_name, schema),
         {:ok, reverse_foreign_keys} <- get_reverse_foreign_keys(connection, table_name, schema),
         {:ok, junction_tables} <- detect_junction_tables(connection, schema) do
      belongs_to = build_associations(foreign_keys)

      primary_key_field = normalize_primary_key(primary_key)

      has_many =
        Enum.into(reverse_foreign_keys, %{}, fn reverse_foreign_key ->
          association_name = Identifier.to_atom!(reverse_foreign_key.referencing_table)
          related_module_name = table_name_to_module(reverse_foreign_key.referencing_table)

          {association_name,
           %{
             type: :has_many,
             association_type: :has_many,
             related_schema: related_module_name,
             related_module_name: related_module_name,
             related_table: reverse_foreign_key.referencing_table,
             queryable: Identifier.to_atom!(reverse_foreign_key.referencing_table),
             field: association_name,
             owner_key: primary_key_field,
             related_key: reverse_foreign_key.referencing_column,
             join_type: :left,
             is_through: false,
             constraint_name: reverse_foreign_key.constraint_name
           }}
        end)

      many_to_many =
        junction_tables
        |> Enum.filter(fn junction -> table_name in junction.tables end)
        |> Enum.flat_map(fn junction ->
          {this_foreign_keys, other_foreign_keys} =
            Enum.split_with(junction.foreign_keys, fn foreign_key ->
              foreign_key.foreign_table_name == table_name
            end)

          Enum.map(other_foreign_keys, fn other_foreign_key ->
            association_name = Identifier.to_atom!(other_foreign_key.foreign_table_name)
            related_module_name = table_name_to_module(other_foreign_key.foreign_table_name)

            owner_foreign_key =
              case this_foreign_keys do
                [foreign_key | _] -> foreign_key.column_name
                _ -> primary_key_field
              end

            {association_name,
             %{
               type: :many_to_many,
               association_type: :many_to_many,
               related_schema: related_module_name,
               related_module_name: related_module_name,
               related_table: other_foreign_key.foreign_table_name,
               queryable: Identifier.to_atom!(other_foreign_key.foreign_table_name),
               field: association_name,
               owner_key: primary_key_field,
               related_key: other_foreign_key.foreign_column_name,
               join_type: :left,
               is_through: false,
               join_through: junction.table,
               join_keys: [
                 {owner_foreign_key, primary_key_field},
                 {other_foreign_key.column_name, other_foreign_key.foreign_column_name}
               ]
             }}
          end)
        end)
        |> Enum.into(%{})

      {:ok, belongs_to |> Map.merge(has_many) |> Map.merge(many_to_many)}
    end
  end

  defp detect_junction_tables(connection, schema) do
    with {:ok, tables} <- list_tables(connection, schema: schema) do
      Enum.reduce_while(tables, {:ok, []}, fn table, {:ok, junction_tables} ->
        case analyze_junction_table(connection, table, schema) do
          {:ok, junction_table} -> {:cont, {:ok, [junction_table | junction_tables]}}
          {:error, :not_junction_table} -> {:cont, {:ok, junction_tables}}
          {:error, reason} -> {:halt, {:error, {:junction_introspection_failed, table, reason}}}
        end
      end)
      |> case do
        {:ok, junction_tables} -> {:ok, Enum.reverse(junction_tables)}
        {:error, _reason} = error -> error
      end
    end
  end

  defp analyze_junction_table(connection, table, schema) do
    with {:ok, columns} <- get_columns(connection, table, schema),
         {:ok, foreign_keys} <- get_foreign_keys(connection, table, schema),
         {:ok, primary_key} <- get_primary_key(connection, table, schema),
         true <- junction_table?(columns, foreign_keys) do
      primary_key_fields = normalize_primary_keys(primary_key)
      foreign_key_fields = Enum.map(foreign_keys, & &1.column_name)
      all_fields = Enum.map(columns, & &1.column_name)

      {:ok,
       %{
         table: table,
         foreign_keys: foreign_keys,
         primary_key: primary_key,
         extra_columns: all_fields -- Enum.uniq(primary_key_fields ++ foreign_key_fields),
         tables: Enum.map(foreign_keys, & &1.foreign_table_name)
       }}
    else
      false -> {:error, :not_junction_table}
      {:error, reason} -> {:error, reason}
    end
  end

  defp junction_table?(columns, foreign_keys) do
    foreign_key_fields = MapSet.new(Enum.map(foreign_keys, & &1.column_name))

    data_fields =
      columns
      |> Enum.map(& &1.column_name)
      |> Enum.reject(fn field ->
        field_name = Atom.to_string(field)

        field_name in ["id", "inserted_at", "updated_at", "created_at"] or
          String.ends_with?(field_name, "_at")
      end)

    length(foreign_keys) == 2 and Enum.all?(data_fields, &MapSet.member?(foreign_key_fields, &1))
  end

  defp normalize_primary_key([primary_key | _]), do: primary_key
  defp normalize_primary_key(primary_key) when is_atom(primary_key), do: primary_key
  defp normalize_primary_key(_), do: :id

  defp normalize_primary_keys(primary_key) when is_list(primary_key), do: primary_key
  defp normalize_primary_keys(primary_key) when is_atom(primary_key), do: [primary_key]
  defp normalize_primary_keys(_), do: []

  defp map_pg_type(connection, data_type, udt_name) do
    case {data_type, udt_name} do
      {"smallint", _} ->
        :integer

      {"integer", _} ->
        :integer

      {"bigint", _} ->
        :integer

      {"smallserial", _} ->
        :integer

      {"serial", _} ->
        :integer

      {"bigserial", _} ->
        :integer

      {"numeric", _} ->
        :decimal

      {"decimal", _} ->
        :decimal

      {"real", _} ->
        :float

      {"double precision", _} ->
        :float

      {"character varying", _} ->
        :string

      {"character", _} ->
        :string

      {"text", _} ->
        :string

      {"citext", _} ->
        :string

      {"boolean", _} ->
        :boolean

      {"date", _} ->
        :date

      {"time without time zone", _} ->
        :time

      {"time with time zone", _} ->
        :time

      {"timestamp without time zone", _} ->
        :naive_datetime

      {"timestamp with time zone", _} ->
        :utc_datetime

      {"json", _} ->
        :jsonb

      {"jsonb", _} ->
        :jsonb

      {"uuid", _} ->
        :binary_id

      {"ARRAY", udt} ->
        {:array, map_pg_type(connection, base_data_type_for_array(udt), normalize_array_udt(udt))}

      {"USER-DEFINED", udt} ->
        map_user_defined_type(connection, udt)

      _ ->
        map_udt_fallback(connection, data_type, udt_name)
    end
  end

  defp map_udt_fallback(connection, _data_type, udt_name) when is_binary(udt_name) do
    case map_user_defined_type(connection, udt_name) do
      :string -> :string
    end
  end

  defp map_udt_fallback(_connection, _data_type, _udt_name), do: :string

  defp map_user_defined_type(connection, udt_name) do
    case get_enum_values(connection, udt_name) do
      {:ok, [_ | _]} -> :string
      _ -> :string
    end
  end

  defp get_enum_values(connection, enum_type_name) do
    query = """
    SELECT e.enumlabel
    FROM pg_type t
    JOIN pg_enum e ON t.oid = e.enumtypid
    WHERE t.typname = $1
    ORDER BY e.enumsortorder
    """

    case introspection_query(connection, query, [enum_type_name]) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [label] -> label end)}
      {:error, reason} -> {:error, {:enum_values_query_failed, reason}}
    end
  end

  defp base_data_type_for_array(<<base::binary>>) do
    case normalize_array_udt(base) do
      "int2" -> "smallint"
      "int4" -> "integer"
      "int8" -> "bigint"
      "varchar" -> "character varying"
      "text" -> "text"
      "bool" -> "boolean"
      "uuid" -> "uuid"
      "jsonb" -> "jsonb"
      "json" -> "json"
      "numeric" -> "numeric"
      "date" -> "date"
      "timestamp" -> "timestamp without time zone"
      "timestamptz" -> "timestamp with time zone"
      _ -> "USER-DEFINED"
    end
  end

  defp normalize_array_udt("_" <> base), do: base
  defp normalize_array_udt(base), do: base

  defp table_name_to_module(table_name) when is_binary(table_name) do
    table_name
    |> singularize()
    |> Macro.camelize()
  end

  defp singularize(word) do
    cond do
      String.ends_with?(word, "ies") ->
        String.replace_suffix(word, "ies", "y")

      String.ends_with?(word, "sses") ->
        String.replace_suffix(word, "sses", "ss")

      String.ends_with?(word, "ses") ->
        String.replace_suffix(word, "ses", "s")

      String.ends_with?(word, "s") and not String.ends_with?(word, "ss") ->
        String.replace_suffix(word, "s", "")

      true ->
        word
    end
  end

  defp validate_pool_connection({:pool, pool_ref}) do
    try do
      case Selecto.ConnectionPool.pool_stats(pool_ref) do
        %{error: _} -> {:error, "Connection pool is not available"}
        stats when is_map(stats) -> :ok
      end
    catch
      :exit, _ -> {:error, "Connection pool is not available"}
    end
  end

  defp pool_stats({:pool, pool_ref}) do
    try do
      Selecto.ConnectionPool.pool_stats(pool_ref)
    catch
      :exit, _ -> %{error: "Pool manager not available"}
    end
  end

  defp execute_with_pool_pid(pool_pid, query, params, cache_key, opts) do
    timeout = Keyword.get(opts, :timeout, 15_000)

    try do
      if cache_key do
        execute_with_prepared_cache(pool_pid, query, params, cache_key, timeout)
      else
        Postgrex.query(pool_pid, query, params, timeout: timeout)
      end
    rescue
      e in DBConnection.ConnectionError ->
        {:error, Selecto.Error.connection_error(Exception.message(e), %{exception: e})}

      e in Postgrex.Error ->
        {:error, Selecto.Error.query_error(Exception.message(e), query, params, %{exception: e})}

      e ->
        {:error, Selecto.Error.query_error(Exception.message(e), query, params, %{exception: e})}
    catch
      :exit, reason ->
        {:error,
         Selecto.Error.connection_error("PostgreSQL connection pool exited", %{
           exit_reason: reason
         })}
    end
  end

  defp execute_with_prepared_cache(pool_pid, query, params, cache_key, timeout) do
    case Selecto.ConnectionPool.prepared_statement_cached?(pool_pid, cache_key) do
      false ->
        result = Postgrex.query(pool_pid, query, params, timeout: timeout)

        if match?({:ok, _}, result) do
          Selecto.ConnectionPool.mark_prepared_statement(pool_pid, cache_key)
        end

        result

      true ->
        Postgrex.query(pool_pid, query, params, timeout: timeout)
    end
  end

  defp start_postgrex_connection(postgrex_opts) do
    case Postgrex.start_link(postgrex_opts) do
      {:ok, pool_pid} -> {:ok, pool_pid, true}
      {:error, {:already_started, pool_pid}} -> {:ok, pool_pid, false}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_result(%{rows: rows, columns: columns} = result) do
    %{
      rows: rows || [],
      columns: Enum.map(columns || [], &to_string/1),
      num_rows: Map.get(result, :num_rows, length(rows || []))
    }
  end

  defp normalize_error_category(code) when code in [:unique_violation, "23505"],
    do: :unique_violation

  defp normalize_error_category(code) when code in [:foreign_key_violation, "23503"],
    do: :foreign_key_violation

  defp normalize_error_category(code) when code in [:not_null_violation, "23502"],
    do: :not_null_violation

  defp normalize_error_category(_code), do: :database_error

  defp enforce_cardinality(%Command{expected_cardinality: expected}, result) do
    affected_rows = Map.get(result, :num_rows, length(Map.get(result, :rows, [])))

    if cardinality_matches?(affected_rows, expected) do
      {:ok, affected_rows}
    else
      {:error,
       Error.new(:cardinality_mismatch, "write affected an unexpected number of rows",
         details: %{expected: expected, actual: affected_rows}
       )}
    end
  end

  defp cardinality_matches?(count, {:exactly, expected}), do: count == expected
  defp cardinality_matches?(count, {:at_most, expected}), do: count <= expected
  defp cardinality_matches?(count, {:at_least, expected}), do: count >= expected

  defp cardinality_matches?(count, {:between, minimum, maximum}),
    do: count >= minimum and count <= maximum

  defp cardinality_matches?(_count, :many), do: true

  defp result_rows(%{rows: rows, columns: columns}) do
    Enum.map(rows, fn row -> Enum.zip(columns, row) |> Map.new() end)
  end

  defp with_postgres_transaction({:pool, pool_ref}, opts, fun) do
    case Selecto.ConnectionPool.get_pool_pid(pool_ref) do
      {:ok, pool_pid} -> with_postgres_transaction(pool_pid, opts, fun)
      {:error, reason} -> {:error, write_error(:transaction_failed, reason)}
    end
  end

  defp with_postgres_transaction(connection, opts, fun)
       when is_atom(connection) and not is_nil(connection) do
    if ecto_repo?(connection) do
      with_ecto_transaction(connection, opts, fun)
    else
      with_native_postgres_transaction(connection, opts, fun)
    end
  end

  defp with_postgres_transaction(connection, opts, fun)
       when is_pid(connection) do
    with_native_postgres_transaction(connection, opts, fun)
  end

  defp with_postgres_transaction(%DBConnection{} = connection, _opts, fun),
    do: fun.(connection)

  defp with_postgres_transaction(connection, _opts, _fun) do
    {:error, write_error(:transaction_failed, {:invalid_connection, connection})}
  end

  defp with_native_postgres_transaction(connection, opts, fun) do
    if valid_postgrex_connection?(connection) do
      case postgres_transaction(connection, opts, fun) do
        {:ok, results} -> {:ok, results}
        {:error, %Error{} = error} -> {:error, error}
        {:error, reason} -> {:error, write_error(:transaction_failed, reason)}
      end
    else
      {:error, write_error(:transaction_failed, {:invalid_connection, connection})}
    end
  end

  defp with_ecto_transaction(repo, opts, fun) do
    transaction_opts = Keyword.take(opts, [:timeout, :log])

    case apply(repo, :transaction, [
           fn -> ecto_transaction_result(repo, fun) end,
           transaction_opts
         ]) do
      {:ok, results} -> {:ok, results}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, write_error(:transaction_failed, reason)}
    end
  rescue
    exception ->
      {:error,
       write_error(
         :transaction_failed,
         {:transaction_exception, exception.__struct__, Exception.message(exception)}
       )}
  catch
    :exit, reason -> {:error, write_error(:transaction_failed, {:connection_exit, reason})}
  end

  defp ecto_transaction_result(repo, fun) do
    case fun.(repo) do
      {:ok, results} -> results
      {:error, error} -> apply(repo, :rollback, [error])
    end
  end

  defp write_error(type, reason) do
    Error.adapter_failure(type, :postgresql, reason, "PostgreSQL write failed")
  end

  defp postgres_transaction(connection, opts, fun) do
    Postgrex.transaction(
      connection,
      fn transaction_connection ->
        case fun.(transaction_connection) do
          {:ok, results} -> results
          {:error, error} -> Postgrex.rollback(transaction_connection, error)
        end
      end,
      opts
    )
  rescue
    exception ->
      {:error, {:transaction_exception, exception.__struct__, Exception.message(exception)}}
  catch
    :exit, reason -> {:error, {:connection_exit, reason}}
  end

  defp resolve_stream_pool_connection(pool_ref) when is_pid(pool_ref) do
    if Process.alive?(pool_ref),
      do: {:ok, pool_ref},
      else: {:error, %{stream_context: :pool, pool_ref: inspect(pool_ref)}}
  end

  defp resolve_stream_pool_connection(pool_ref) when is_atom(pool_ref) and not is_nil(pool_ref) do
    if valid_named_connection?(pool_ref),
      do: {:ok, pool_ref},
      else: {:error, %{stream_context: :pool, pool_ref: inspect(pool_ref)}}
  end

  defp resolve_stream_pool_connection(%{pool: pool_conn})
       when is_pid(pool_conn) or is_atom(pool_conn) do
    resolve_stream_pool_connection(pool_conn)
  end

  defp resolve_stream_pool_connection(pool_ref) do
    {:error, %{stream_context: :pool, pool_ref: inspect(pool_ref)}}
  end

  defp build_postgrex_cursor_stream(conn, query, params, opts) do
    query = normalize_query(query)
    max_rows = Keyword.get(opts, :max_rows, 500)
    stream_timeout = Keyword.get(opts, :stream_timeout, 30_000)
    receive_timeout = Keyword.get(opts, :receive_timeout, 60_000)
    queue_timeout = Keyword.get(opts, :queue_timeout, 100)

    producer =
      Keyword.get(opts, :stream_producer, fn send_chunk ->
        Postgrex.transaction(
          conn,
          fn tx_conn ->
            tx_conn
            |> Postgrex.stream(query, params, max_rows: max_rows)
            |> Enum.each(fn %Postgrex.Result{rows: rows, columns: columns} ->
              send_chunk.(rows, columns)
            end)
          end,
          timeout: stream_timeout
        )
      end)

    Stream.resource(
      fn ->
        parent = self()
        ref = make_ref()

        task =
          Selecto.TaskSupervisor.async(fn ->
            run_stream_producer(parent, ref, producer)
          end)

        %{task: task, ref: ref, monitor_ref: task.ref, awaiting_ack?: false}
      end,
      fn state ->
        state = acknowledge_stream_chunk(state)
        ref = state.ref
        monitor_ref = state.monitor_ref

        receive do
          {^ref, {:chunk, rows, columns}} ->
            stream_rows = Enum.map(rows, &{&1, columns || []})
            {stream_rows, %{state | awaiting_ack?: true}}

          {^ref, {:done, {:ok, _}}} ->
            {:halt, state}

          {^ref, {:done, {:error, reason}}} ->
            raise "PostgreSQL stream transaction failed: #{inspect(reason)}"

          {^ref, {:producer_failed, reason}} ->
            raise "PostgreSQL stream producer failed: #{inspect(reason)}"

          {^monitor_ref, _reply} ->
            {:halt, state}

          {:DOWN, ^monitor_ref, :process, _pid, :normal} ->
            {:halt, state}

          {:DOWN, ^monitor_ref, :process, _pid, reason} ->
            raise "PostgreSQL stream producer failed: #{inspect(reason)}"
        after
          receive_timeout ->
            raise "Timed out waiting for streamed rows after #{receive_timeout}ms"
        end
      end,
      fn state ->
        stop_stream_producer(state, queue_timeout)
      end
    )
  end

  defp run_stream_producer(parent, ref, producer) do
    parent_monitor_ref = Process.monitor(parent)

    tx_result =
      try do
        producer.(fn rows, columns ->
          send(parent, {ref, {:chunk, rows, columns}})
          await_stream_ack(parent, parent_monitor_ref, ref)
        end)
      rescue
        exception ->
          {:producer_failed, {:exception, exception.__struct__, Exception.message(exception)}}
      catch
        :throw, {:selecto_stream_cancelled, ^ref} ->
          {:consumer_cancelled, ref}

        kind, reason ->
          {:producer_failed, {kind, reason}}
      after
        Process.demonitor(parent_monitor_ref, [:flush])
      end

    case tx_result do
      {:consumer_cancelled, ^ref} -> :ok
      {:producer_failed, reason} -> send(parent, {ref, {:producer_failed, reason}})
      result -> send(parent, {ref, {:done, result}})
    end
  end

  defp await_stream_ack(parent, parent_monitor_ref, ref) do
    receive do
      {^ref, :ack} ->
        :ok

      {^ref, :cancel} ->
        throw({:selecto_stream_cancelled, ref})

      {:DOWN, ^parent_monitor_ref, :process, ^parent, _reason} ->
        throw({:selecto_stream_cancelled, ref})
    end
  end

  defp acknowledge_stream_chunk(%{awaiting_ack?: false} = state), do: state

  defp acknowledge_stream_chunk(%{task: task, ref: ref, awaiting_ack?: true} = state) do
    send(task.pid, {ref, :ack})
    %{state | awaiting_ack?: false}
  end

  defp stop_stream_producer(state, queue_timeout) do
    send(state.task.pid, {state.ref, :cancel})
    # Give the cancellation handshake time to unwind the cursor transaction.
    # Task.shutdown/2 sends an exit immediately and can discard the DB session
    # before Postgrex has rolled back and returned it to the pool.
    _result = Task.yield(state.task, queue_timeout) || Task.shutdown(state.task, :brutal_kill)
    Process.demonitor(state.monitor_ref, [:flush])
    drain_stream_messages(state.ref)
  end

  defp drain_stream_messages(ref) do
    receive do
      {^ref, _message} -> drain_stream_messages(ref)
    after
      0 -> :ok
    end
  end

  defp fetch_server_version_num({:pool, pool_ref}) do
    try do
      case Selecto.ConnectionPool.execute(pool_ref, @server_version_num_query, [],
             prepared: false
           ) do
        {:ok, result} -> extract_server_version_num(result)
        {:error, _reason} = error -> error
      end
    catch
      :exit, _reason -> {:error, :pool_unavailable}
    end
  end

  defp fetch_server_version_num(connection) when is_atom(connection) do
    cond do
      function_exported?(connection, :query, 2) ->
        case apply(connection, :query, [@server_version_num_query, []]) do
          {:ok, result} -> extract_server_version_num(result)
          {:error, _reason} = error -> error
          _other -> {:error, :invalid_query_result}
        end

      is_pid(Process.whereis(connection)) ->
        fetch_server_version_num_with_postgrex(connection)

      true ->
        {:error, :unsupported_connection}
    end
  end

  defp fetch_server_version_num(connection) when is_pid(connection) do
    fetch_server_version_num_with_postgrex(connection)
  end

  defp fetch_server_version_num(connection) when is_list(connection) do
    case Postgrex.start_link(Keyword.put_new(connection, :supervisor, false)) do
      {:ok, pid} ->
        result = fetch_server_version_num_with_postgrex(pid)
        GenServer.stop(pid)
        result

      {:error, _reason} = error ->
        error
    end
  end

  defp fetch_server_version_num(%DBConnection{} = connection) do
    fetch_server_version_num_with_postgrex(connection)
  end

  defp fetch_server_version_num(connection) when is_map(connection) do
    connection
    |> Map.to_list()
    |> fetch_server_version_num()
  end

  defp fetch_server_version_num(_connection), do: {:error, :unsupported_connection}

  defp fetch_server_version_num_with_postgrex(connection) do
    case Postgrex.query(connection, @server_version_num_query, []) do
      {:ok, result} -> extract_server_version_num(result)
      {:error, _reason} = error -> error
    end
  rescue
    _ -> {:error, :query_failed}
  end

  defp extract_server_version_num(%{rows: [[value | _] | _]}) do
    parse_server_version_num(value)
  end

  defp extract_server_version_num(_result), do: {:error, :missing_server_version_num}

  defp parse_server_version_num(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp parse_server_version_num(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed > 0 -> {:ok, parsed}
      _ -> {:error, :invalid_server_version_num}
    end
  end

  defp parse_server_version_num(_value), do: {:error, :invalid_server_version_num}
end
