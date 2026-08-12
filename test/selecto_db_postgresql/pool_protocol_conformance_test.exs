defmodule SelectoDBPostgreSQL.PoolProtocolConformanceTest do
  use ExUnit.Case, async: false

  alias Selecto.ConnectionPool
  alias SelectoDBPostgreSQL.Adapter

  defmodule IdlePool do
    use GenServer

    def start, do: GenServer.start(__MODULE__, :ok)
    @impl true
    def init(:ok), do: {:ok, %{}}
  end

  test "managed pool start reuse kill retirement restart and stop follow generations" do
    pool_name =
      ConnectionPool.generate_pool_name(
        adapter: Adapter,
        database: "pool_protocol_#{System.unique_integer([:positive])}"
      )

    first_pool = idle_process()
    first_opts = manager_opts(first_pool, pool_name)

    assert {:ok, first_manager, :started} = ConnectionPool.start_manager(first_opts)
    assert {:ok, ^first_manager, :existing} = ConnectionPool.start_manager(first_opts)

    assert {:ok, %{pool: ^first_pool, manager: ^first_manager}} =
             ConnectionPool.build_pool_ref_from_manager(first_manager)

    Process.exit(first_pool, :kill)
    assert eventually(fn -> not Process.alive?(first_pool) end)

    dead_ref = %{adapter: Adapter, pool: first_pool, manager: first_manager, name: pool_name}

    assert {:error, %Selecto.Error{type: :connection_error}} =
             Adapter.execute({:pool, dead_ref}, "select 1", [], [])

    assert :error = ConnectionPool.get_manager_pid_by_name(pool_name)
    assert eventually(fn -> not Process.alive?(first_manager) end)

    second_pool = idle_process()
    second_opts = manager_opts(second_pool, pool_name)

    assert {:ok, second_manager, :started} = ConnectionPool.start_manager(second_opts)
    assert second_manager != first_manager

    assert {:ok, second_ref} = ConnectionPool.build_pool_ref_from_manager(second_manager)
    assert second_ref.pool == second_pool

    assert :ok = ConnectionPool.stop_pool(second_ref)
    assert eventually(fn -> ConnectionPool.get_manager_pid_by_name(pool_name) == :error end)
    refute Process.alive?(second_pool)
  end

  test "dead pool transaction and with-connection callbacks fail closed without invoking callers" do
    pool = spawn(fn -> :ok end)
    monitor = Process.monitor(pool)
    assert_receive {:DOWN, ^monitor, :process, ^pool, _reason}

    pool_ref = %{adapter: Adapter, pool: pool}

    assert {:error, %Selecto.Error{type: :connection_error}} =
             Adapter.transaction(pool_ref, fn _connection -> flunk("callback must not run") end)

    assert {:error, %Selecto.Error{type: :connection_error}} =
             Adapter.with_connection(pool_ref, fn _connection ->
               flunk("callback must not run")
             end)
  end

  @tag :postgres
  test "real PostgreSQL pool is reused retired and replaced after pool death" do
    connection_options = SelectoDBPostgreSQL.Verification.ConnectionOptions.options()
    pool_options = [adapter: Adapter, pool_size: 1, max_overflow: 0]

    assert {:ok, first_ref} = ConnectionPool.start_pool(connection_options, pool_options)
    Process.unlink(first_ref.pool)

    on_exit(fn ->
      if Process.alive?(first_ref.manager) or Process.alive?(first_ref.pool) do
        ConnectionPool.stop_pool(first_ref)
      end
    end)

    assert {:ok, reused_ref} = ConnectionPool.start_pool(connection_options, pool_options)
    assert reused_ref.manager == first_ref.manager
    assert reused_ref.pool == first_ref.pool

    assert {:ok, %{rows: [[1]]}} = Adapter.execute({:pool, first_ref}, "SELECT 1", [], [])

    Process.exit(first_ref.pool, :kill)
    assert eventually(fn -> not Process.alive?(first_ref.pool) end)

    assert {:error, %Selecto.Error{type: :connection_error}} =
             Adapter.execute({:pool, first_ref}, "SELECT 1", [], [])

    assert {:ok, replacement_ref} = ConnectionPool.start_pool(connection_options, pool_options)
    Process.unlink(replacement_ref.pool)

    on_exit(fn ->
      if Process.alive?(replacement_ref.manager) or Process.alive?(replacement_ref.pool) do
        ConnectionPool.stop_pool(replacement_ref)
      end
    end)

    refute replacement_ref.manager == first_ref.manager
    refute replacement_ref.pool == first_ref.pool
    assert eventually(fn -> not Process.alive?(first_ref.manager) end)

    assert {:ok, %{rows: [[1]]}} =
             Adapter.execute({:pool, replacement_ref}, "SELECT 1", [], [])

    assert :ok = ConnectionPool.stop_pool(replacement_ref)
    assert eventually(fn -> not Process.alive?(replacement_ref.manager) end)
    assert eventually(fn -> not Process.alive?(replacement_ref.pool) end)
  end

  defp manager_opts(pool, name) do
    [
      adapter: Adapter,
      pool_pid: pool,
      pool_name: name,
      pool_config: [pool_size: 1],
      connection_config: [database: "protocol"]
    ]
  end

  defp idle_process do
    {:ok, pool} = IdlePool.start()
    pool
  end

  defp eventually(fun, attempts \\ 50)

  defp eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end

  defp eventually(_fun, 0), do: false
end
