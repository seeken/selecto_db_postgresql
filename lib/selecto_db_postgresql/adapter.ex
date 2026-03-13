defmodule SelectoDBPostgreSQL.Adapter do
  @moduledoc """
  PostgreSQL adapter for Selecto.
  """

  @behaviour Selecto.DB.Adapter

  @impl true
  def name, do: :postgresql

  @impl true
  def connect({:pool, _} = pool_ref), do: {:ok, pool_ref}
  def connect(connection) when is_pid(connection) or is_atom(connection), do: {:ok, connection}
  def connect(opts) when is_map(opts), do: connect(Map.to_list(opts))

  def connect(opts) when is_list(opts) do
    case Postgrex.start_link(opts) do
      {:ok, conn} -> {:ok, conn}
      {:error, reason} -> {:error, reason}
    end
  end

  def connect(other), do: {:error, {:invalid_connection_options, other}}

  @impl true
  def execute({:pool, pool_ref}, query, params, opts) do
    case Selecto.ConnectionPool.execute(pool_ref, normalize_query(query), params, opts) do
      {:ok, result} -> {:ok, normalize_result(result)}
      {:error, reason} -> {:error, reason}
    end
  end

  def execute(connection, query, params, opts) when is_pid(connection) or is_atom(connection) do
    case Postgrex.query(connection, normalize_query(query), params, opts) do
      {:ok, result} -> {:ok, normalize_result(result)}
      {:error, reason} -> {:error, reason}
    end
  end

  def execute(connection, _query, _params, _opts), do: {:error, {:invalid_connection, connection}}

  @impl true
  def placeholder(index), do: ["$", Integer.to_string(index)]

  @impl true
  def quote_identifier(identifier) when is_binary(identifier) do
    escaped = String.replace(identifier, "\"", "\"\"")
    "\"#{escaped}\""
  end

  def quote_identifier(identifier), do: identifier |> to_string() |> quote_identifier()

  @impl true
  def supports?(feature) do
    feature in [
      :cte,
      :jsonb,
      :array_ops,
      :returning,
      :window_functions,
      :lateral_join,
      :prefix,
      :stream
    ]
  end

  @impl true
  def stream({:pool, pool_ref}, query, params, opts) do
    case resolve_stream_pool_connection(pool_ref) do
      {:ok, pool_conn} -> {:ok, build_postgrex_cursor_stream(pool_conn, query, params, opts)}
      {:error, details} -> {:error, {:invalid_stream_pool, details}}
    end
  end

  def stream(conn, query, params, opts) when is_pid(conn) or is_atom(conn) do
    {:ok, build_postgrex_cursor_stream(conn, query, params, opts)}
  end

  def stream(connection, _query, _params, _opts) do
    {:error, {:invalid_connection, connection}}
  end

  @server_version_num_query "show server_version_num"

  @impl true
  def server_version_major(connection) do
    with {:ok, version_num} <- fetch_server_version_num(connection),
         true <- is_integer(version_num) and version_num > 0 do
      {:ok, div(version_num, 10_000)}
    else
      false -> {:error, :invalid_server_version_num}
      {:error, _reason} = error -> error
      _ -> {:error, :invalid_server_version_num}
    end
  end

  defp normalize_query(query) when is_binary(query), do: query
  defp normalize_query(query), do: IO.iodata_to_binary(query)

  defp normalize_result(%{rows: rows, columns: columns}) do
    %{
      rows: rows || [],
      columns: Enum.map(columns || [], &to_string/1)
    }
  end

  defp resolve_stream_pool_connection(pool_ref) when is_pid(pool_ref) or is_atom(pool_ref) do
    {:ok, pool_ref}
  end

  defp resolve_stream_pool_connection(%{pool: pool_conn})
       when is_pid(pool_conn) or is_atom(pool_conn) do
    {:ok, pool_conn}
  end

  defp resolve_stream_pool_connection(pool_ref) do
    {:error, %{stream_context: :pool, pool_ref: inspect(pool_ref)}}
  end

  defp build_postgrex_cursor_stream(conn, query, params, opts) do
    parent = self()
    ref = make_ref()
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
        task =
          Selecto.TaskSupervisor.async(fn ->
            tx_result =
              producer.(fn rows, columns ->
                send(parent, {ref, {:chunk, rows, columns}})
              end)

            send(parent, {ref, {:done, tx_result}})
          end)

        %{task: task, ref: ref}
      end,
      fn state ->
        ref = state.ref

        receive do
          {^ref, {:chunk, rows, columns}} ->
            stream_rows = Enum.map(rows, &{&1, columns || []})
            {stream_rows, state}

          {^ref, {:done, {:ok, _}}} ->
            {:halt, state}

          {^ref, {:done, {:error, reason}}} ->
            raise "PostgreSQL stream transaction failed: #{inspect(reason)}"
        after
          receive_timeout ->
            raise "Timed out waiting for streamed rows after #{receive_timeout}ms"
        end
      end,
      fn state ->
        case Task.shutdown(state.task, queue_timeout) do
          nil -> :ok
          {:exit, _} -> :ok
          _ -> :ok
        end
      end
    )
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

  defp parse_server_version_num(value) when is_integer(value), do: {:ok, value}

  defp parse_server_version_num(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} -> {:ok, parsed}
      _ -> {:error, :invalid_server_version_num}
    end
  end

  defp parse_server_version_num(_value), do: {:error, :invalid_server_version_num}
end
