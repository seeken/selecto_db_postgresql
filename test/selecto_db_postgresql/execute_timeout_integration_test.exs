defmodule SelectoDBPostgreSQL.ExecuteTimeoutIntegrationTest do
  @moduledoc """
  The adapter's `:execute_timeout` contract against a live PostgreSQL
  database: `Selecto.execute/2` runs the statement in the calling process,
  Postgrex gets the shorter of Selecto's remaining time and the timeout that
  applied before (15 seconds, or an Ecto repository's own), and the rows and
  the timeout error match the task path for every connection type.
  """

  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  @moduletag :postgres

  # The adapter without the opt-in: Selecto runs the query in its task.
  defmodule TaskPathAdapter do
    @moduledoc false
    @delegate SelectoDBPostgreSQL.Adapter

    for {function, arity} <- @delegate.__info__(:functions), function != :supports? do
      args = Macro.generate_arguments(arity, __MODULE__)

      def unquote(function)(unquote_splicing(args)),
        do: @delegate.unquote(function)(unquote_splicing(args))
    end

    def supports?(:execute_timeout), do: false
    def supports?(feature), do: @delegate.supports?(feature)
  end

  defmodule Repo do
    use Ecto.Repo, otp_app: :selecto_db_postgresql, adapter: Ecto.Adapters.Postgres
  end

  @adapters [TaskPathAdapter, Adapter]

  setup_all do
    {:ok, conn} = Postgrex.start_link(ConnectionOptions.options())
    suffix = System.unique_integer([:positive])
    items = "selecto_exec_timeout_items_#{suffix}"
    slow = "selecto_exec_timeout_slow_#{suffix}"

    Postgrex.query!(
      conn,
      "CREATE TABLE #{items} (id integer PRIMARY KEY, name text, price numeric(10,2), " <>
        "added_at timestamp)",
      []
    )

    Postgrex.query!(
      conn,
      "INSERT INTO #{items} SELECT g, 'item ' || g, g * 1.25, " <>
        "timestamp '2026-01-01' + g * interval '1 hour' FROM generate_series(1, 2000) g",
      []
    )

    # Reading the view takes at least one second.
    Postgrex.query!(
      conn,
      "CREATE VIEW #{slow} AS SELECT id, name, price, added_at FROM #{items}, " <>
        "pg_sleep(1) WHERE id <= 3",
      []
    )

    GenServer.stop(conn)

    on_exit(fn ->
      {:ok, cleanup} = Postgrex.start_link(ConnectionOptions.options())
      Postgrex.query!(cleanup, "DROP VIEW IF EXISTS #{slow}", [])
      Postgrex.query!(cleanup, "DROP TABLE IF EXISTS #{items}", [])
      GenServer.stop(cleanup)
    end)

    {:ok, items: items, slow: slow}
  end

  setup do
    {:ok, conn} = Postgrex.start_link(Keyword.put(ConnectionOptions.options(), :pool_size, 2))
    Process.unlink(conn)
    on_exit(fn -> if Process.alive?(conn), do: GenServer.stop(conn) end)
    {:ok, conn: conn}
  end

  test "the adapter declares that execute/4 enforces the timeout" do
    assert Adapter.supports?(:execute_timeout)
    assert Adapter.capability(:execute_timeout).supported?
  end

  test "Selecto.execute/2 runs Postgrex in the calling process with the 15 second cap",
       %{conn: conn, items: items} do
    {result, calls} = trace_calls({Postgrex, :query, 4}, fn -> execute(conn, items, Adapter) end)

    assert {:ok, {rows, _columns, _aliases}} = result
    assert length(rows) == 2000

    # The call happened in this process, so no result was copied out of a task.
    assert [[^conn, _sql, _params, opts]] = calls
    assert opts[:timeout] == 15_000
    assert is_integer(opts[:deadline])
  end

  test "without the opt-in the query runs in Selecto's task", %{conn: conn, items: items} do
    {result, calls} =
      trace_calls({Postgrex, :query, 4}, fn -> execute(conn, items, TaskPathAdapter) end)

    assert {:ok, _result} = result
    assert calls == []
  end

  test "a shorter Selecto timeout is passed through instead of the cap",
       %{conn: conn, items: items} do
    {result, calls} =
      trace_calls({Postgrex, :query, 4}, fn -> execute(conn, items, Adapter, timeout: 5_000) end)

    assert {:ok, _result} = result
    assert [[^conn, _sql, _params, opts]] = calls
    assert opts[:timeout] <= 5_000 and opts[:timeout] > 4_000
  end

  test "a longer Selecto timeout never lengthens the 15 second cap",
       %{conn: conn, items: items} do
    {_result, calls} =
      trace_calls({Postgrex, :query, 4}, fn -> execute(conn, items, Adapter, timeout: 60_000) end)

    assert [[_conn, _sql, _params, opts]] = calls
    assert opts[:timeout] == 15_000
  end

  test "both paths return the same rows", %{conn: conn, items: items} do
    [task_result, caller_result] = for adapter <- @adapters, do: execute(conn, items, adapter)

    assert {:ok, {rows, ["id", "name", "price", "added_at"], _aliases}} = task_result
    assert hd(rows) == [1, "item 1", Decimal.new("1.25"), ~N[2026-01-01 01:00:00.000000]]
    assert task_result == caller_result
  end

  test "a query sleeping past a shorter timeout returns the task path's timeout error",
       %{conn: conn, slow: slow} do
    [task_error, caller_error] =
      for adapter <- @adapters do
        assert_abandoned(fn -> execute(conn, slow, adapter, timeout: 100) end)
      end

    assert error_shape(task_error) == error_shape(caller_error)
  end

  test "repeated short timeouts always yield the timeout error", %{conn: conn, slow: slow} do
    for _attempt <- 1..20 do
      assert_abandoned(fn -> execute(conn, slow, Adapter, timeout: 50) end, 50)
    end
  end

  test "a managed pool gets the same cap and abandons the statement", %{items: items, slow: slow} do
    pool_name = :"selecto_exec_timeout_pool_#{System.unique_integer([:positive])}"

    {:ok, pool_ref} =
      Adapter.start_pool(
        ConnectionOptions.options(),
        %{pool_size: 2, max_overflow: 0, connection_timeout: 5_000, checkout_timeout: 5_000},
        pool_name
      )

    on_exit(fn -> Selecto.ConnectionPool.stop_pool(pool_ref) end)
    pool = {:pool, pool_ref}
    sql = "SELECT id FROM #{items} ORDER BY id"

    {result, calls} =
      trace_calls({Postgrex, :query, 4}, fn -> Adapter.execute(pool, sql, [], timeout: 30_000) end)

    assert {:ok, %{rows: rows}} = result
    assert length(rows) == 2000
    assert [[_pool_pid, ^sql, [], opts]] = calls
    assert opts[:timeout] == 15_000
    assert is_integer(opts[:deadline])
    assert Adapter.execute(pool, sql, [], []) == result

    started = System.monotonic_time(:millisecond)

    # Postgrex cancels the statement when the timeout elapses.
    assert {:error, %Postgrex.Error{postgres: %{code: :query_canceled}}} =
             Adapter.execute(pool, "SELECT * FROM #{slow}", [], timeout: 100)

    assert System.monotonic_time(:millisecond) - started < 900

    assert {:ok, {rows, _columns, _aliases}} = execute(pool, items, Adapter)
    assert length(rows) == 2000
    assert_abandoned(fn -> execute(pool, slow, Adapter, timeout: 100) end)
  end

  test "an Ecto repository gets the shorter of the timeout and its configured timeout",
       %{items: items, slow: slow} do
    start_repo(timeout: 4_000)

    {result, calls} =
      trace_calls({Ecto.Adapters.SQL, :query, 4}, fn -> execute(Repo, items, Adapter) end)

    assert {:ok, {rows, _columns, _aliases}} = result
    assert length(rows) == 2000
    assert [[Repo, _sql, _params, opts]] = calls
    assert opts[:timeout] == 4_000

    assert execute(Repo, items, TaskPathAdapter) == result

    [task_error, caller_error] =
      for adapter <- @adapters do
        assert_abandoned(fn -> execute(Repo, slow, adapter, timeout: 100) end)
      end

    assert error_shape(task_error) == error_shape(caller_error)
  end

  test "an Ecto repository without a configured timeout gets the 15 second cap",
       %{items: items} do
    start_repo([])

    {_result, calls} =
      trace_calls({Ecto.Adapters.SQL, :query, 4}, fn -> execute(Repo, items, Adapter) end)

    assert [[Repo, _sql, _params, opts]] = calls
    assert opts[:timeout] == 15_000
  end

  test "inside the repository's transaction the statement still honours the timeout",
       %{slow: slow} do
    start_repo(timeout: 60_000)

    {:ok, outcome} =
      Repo.transaction(
        fn ->
          started = System.monotonic_time(:millisecond)
          result = Adapter.execute(Repo, "SELECT * FROM #{slow}", [], timeout: 100)
          {result, System.monotonic_time(:millisecond) - started}
        end,
        timeout: 60_000
      )

    assert {{:error, %DBConnection.ConnectionError{}}, elapsed} = outcome
    assert elapsed < 900
  end

  test "a checked-out Postgrex connection honours the timeout", %{conn: conn, slow: slow} do
    parent = self()

    # The abandoned statement leaves the checked-out connection unusable, as
    # shutting down Selecto's task did, so the transaction's end may fail.
    try do
      Postgrex.transaction(
        conn,
        fn checked_out ->
          started = System.monotonic_time(:millisecond)
          result = Adapter.execute(checked_out, "SELECT * FROM #{slow}", [], timeout: 100)
          send(parent, {:outcome, result, System.monotonic_time(:millisecond) - started})
          Postgrex.rollback(checked_out, :done)
        end,
        timeout: 60_000
      )
    rescue
      _exception -> :ok
    catch
      :exit, _reason -> :ok
    end

    assert_receive {:outcome, {:error, %DBConnection.ConnectionError{}}, elapsed}, 5_000
    assert elapsed < 900
  end

  test "a checked-out Postgrex connection returns rows within the timeout", %{conn: conn} do
    assert {:ok, {:ok, %{rows: [[1]]}}} =
             Postgrex.transaction(conn, fn checked_out ->
               Adapter.execute(checked_out, "SELECT 1", [], timeout: 5_000)
             end)
  end

  test "named Postgrex connections get the cap", %{items: items} do
    name = :"selecto_exec_timeout_named_#{System.unique_integer([:positive])}"
    {:ok, named} = Postgrex.start_link(Keyword.put(ConnectionOptions.options(), :name, name))
    Process.unlink(named)
    on_exit(fn -> if Process.alive?(named), do: GenServer.stop(named) end)

    {result, calls} = trace_calls({Postgrex, :query, 4}, fn -> execute(name, items, Adapter) end)
    assert {:ok, _result} = result
    assert [[^name, _sql, _params, opts]] = calls
    assert opts[:timeout] == 15_000
  end

  test "execute/4 without a timeout keeps the driver's own default", %{conn: conn} do
    {result, calls} =
      trace_calls({Postgrex, :query, 4}, fn -> Adapter.execute(conn, "SELECT 1", [], []) end)

    assert {:ok, %{rows: [[1]]}} = result
    assert [[^conn, "SELECT 1", [], []]] = calls
  end

  defp start_repo(config) do
    config =
      ConnectionOptions.options()
      |> Keyword.merge(pool_size: 2, log: false)
      |> Keyword.merge(config)

    start_supervised!({Repo, config})
  end

  defp execute(connection, table, adapter, opts \\ []) do
    domain = %{
      name: "Execute timeout",
      source: %{
        source_table: table,
        primary_key: :id,
        fields: [:id, :name, :price, :added_at],
        redact_fields: [],
        columns: %{
          id: %{type: :integer},
          name: %{type: :string},
          price: %{type: :decimal},
          added_at: %{type: :naive_datetime}
        },
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }

    # The fixed rollup setting keeps configure from querying the server.
    Selecto.configure(domain, connection, adapter: adapter, rollup_sort_fix: false)
    |> Selecto.select(["id", "name", "price", "added_at"])
    |> Selecto.filter({"id", {:gt, 0}})
    |> Selecto.order_by(["id"])
    |> Selecto.execute(opts)
  end

  defp assert_abandoned(fun, timeout \\ 100) do
    started = System.monotonic_time(:millisecond)
    {result, log} = with_log(fun)

    assert {:error, %Selecto.Error{type: :timeout_error} = error} = result
    assert error.message == "Query exceeded timeout of #{timeout}ms"
    assert %{timeout: ^timeout, duration: duration} = error.details
    assert duration >= timeout
    assert log =~ "[Selecto] Query timeout after #{timeout}ms"

    # The statement was abandoned, not waited for.
    assert System.monotonic_time(:millisecond) - started < 900
    error
  end

  defp error_shape(%Selecto.Error{} = error) do
    {error.type, error.message, error.details |> Map.keys() |> Enum.sort(), error.details.timeout}
  end

  # Calls to `mfa` made by this process (not by any task it starts).
  defp trace_calls({module, function, arity} = mfa, fun) do
    Code.ensure_loaded!(module)
    tracer = spawn_link(fn -> collect_traces([]) end)
    1 = :erlang.trace_pattern(mfa, true, [:global])
    1 = :erlang.trace(self(), true, [:call, {:tracer, tracer}])

    result =
      try do
        fun.()
      after
        :erlang.trace(self(), false, [:call])
        :erlang.trace_pattern(mfa, false, [:global])
      end

    delivered = :erlang.trace_delivered(self())
    assert_receive {:trace_delivered, _pid, ^delivered}, 1_000
    send(tracer, {:collect, self()})
    assert_receive {:traces, traces}, 1_000

    calls =
      for {:trace, _pid, :call, {^module, ^function, args}} <- traces,
          length(args) == arity,
          do: args

    {result, calls}
  end

  defp collect_traces(traces) do
    receive do
      {:collect, from} -> send(from, {:traces, Enum.reverse(traces)})
      trace -> collect_traces([trace | traces])
    end
  end
end
