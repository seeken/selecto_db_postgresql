defmodule SelectoDBPostgreSQL.StatementCacheTest do
  @moduledoc """
  The opt-in `:statement_cache` setting: statements are prepared once per
  connection under a hash-slot name, so a repeated statement costs one round
  trip (Bind/Execute) instead of two (Parse/Describe, then Bind/Execute).

  Round trips are counted by tracing the socket sends Postgrex makes.
  """

  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  setup do
    previous = Application.fetch_env(:selecto_db_postgresql, :statement_cache)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:selecto_db_postgresql, :statement_cache, value)
        :error -> Application.delete_env(:selecto_db_postgresql, :statement_cache)
      end
    end)

    :ok
  end

  describe "statement_cache_opts/2" do
    test "leaves options alone when the cache is off (the default)" do
      Application.delete_env(:selecto_db_postgresql, :statement_cache)
      assert Adapter.statement_cache_opts("SELECT 1", timeout: 5) == [timeout: 5]

      for off <- [false, nil, 0, -1, :yes] do
        Application.put_env(:selecto_db_postgresql, :statement_cache, off)
        assert Adapter.statement_cache_opts("SELECT 1", []) == []
      end
    end

    test "names a statement by a hash slot of its SQL" do
      Application.put_env(:selecto_db_postgresql, :statement_cache, true)
      opts = Adapter.statement_cache_opts("SELECT 1", timeout: 5)
      name = Keyword.fetch!(opts, :cache_statement)

      assert Keyword.fetch!(opts, :timeout) == 5
      assert name == "selecto_#{:erlang.phash2("SELECT 1", 256)}"
      assert Adapter.statement_cache_opts("SELECT 1", []) == [cache_statement: name]

      Application.put_env(:selecto_db_postgresql, :statement_cache, 1)

      assert Adapter.statement_cache_opts("SELECT 1", []) ==
               Adapter.statement_cache_opts("SELECT 2", [])
    end

    test "keeps commented, explicitly cached and unprepared statements unnamed" do
      Application.put_env(:selecto_db_postgresql, :statement_cache, true)

      assert Adapter.statement_cache_opts("SELECT 1", comment: "x") == [comment: "x"]

      assert Adapter.statement_cache_opts("SELECT 1", cache_statement: "q") == [
               cache_statement: "q"
             ]

      assert Adapter.statement_cache_opts("SELECT 1", prepared: false) == [prepared: false]
    end
  end

  describe "live connections" do
    @describetag :postgres

    setup do
      {:ok, conn} = Postgrex.start_link(Keyword.put(ConnectionOptions.options(), :pool_size, 1))
      Process.unlink(conn)
      on_exit(fn -> if Process.alive?(conn), do: GenServer.stop(conn) end)
      # Connected and its types bootstrapped, so only the statements count.
      Postgrex.query!(conn, "SELECT $1::int", [0])
      %{conn: conn}
    end

    test "off, every execution prepares again", %{conn: conn} do
      Application.put_env(:selecto_db_postgresql, :statement_cache, false)

      for _ <- 1..2 do
        {result, frames} =
          socket_frames(fn -> Adapter.execute(conn, "SELECT $1::int", [7], []) end)

        assert {:ok, %{rows: [[7]]}} = result
        assert length(frames) == 2
      end
    end

    test "on, a repeated statement takes one round trip", %{conn: conn} do
      Application.put_env(:selecto_db_postgresql, :statement_cache, true)

      {first, frames} = socket_frames(fn -> Adapter.execute(conn, "SELECT $1::int", [7], []) end)
      assert {:ok, %{rows: [[7]], columns: ["int4"]}} = first
      assert length(frames) == 2

      {again, frames} = socket_frames(fn -> Adapter.execute(conn, "SELECT $1::int", [8], []) end)
      assert {:ok, %{rows: [[8]]}} = again
      assert length(frames) == 1
    end

    test "on, statements sharing a slot replace each other and return their own rows",
         %{conn: conn} do
      Application.put_env(:selecto_db_postgresql, :statement_cache, 1)

      for _ <- 1..2 do
        assert {:ok, %{rows: [[1]]}} = Adapter.execute(conn, "SELECT 1", [], [])
        assert {:ok, %{rows: [["two"]]}} = Adapter.execute(conn, "SELECT 'two'", [], [])
      end
    end

    test "on, a statement whose result type changed is prepared again", %{conn: conn} do
      Application.put_env(:selecto_db_postgresql, :statement_cache, true)
      table = "selecto_statement_cache_#{System.unique_integer([:positive])}"
      Postgrex.query!(conn, "CREATE TABLE #{table} (id integer)", [])
      on_exit(fn -> drop_table(table) end)
      Postgrex.query!(conn, "INSERT INTO #{table} VALUES (1)", [])

      sql = "SELECT * FROM #{table}"
      assert {:ok, %{rows: [[1]]}} = Adapter.execute(conn, sql, [], [])

      Postgrex.query!(conn, "ALTER TABLE #{table} ADD COLUMN name text DEFAULT 'a'", [])
      assert {:ok, %{rows: [[1, "a"]]}} = Adapter.execute(conn, sql, [], [])
    end

    test "on, writes run in the adapter's transaction as before", %{conn: conn} do
      Application.put_env(:selecto_db_postgresql, :statement_cache, true)
      table = "selecto_statement_cache_#{System.unique_integer([:positive])}"
      Postgrex.query!(conn, "CREATE TABLE #{table} (id integer PRIMARY KEY, n integer)", [])
      on_exit(fn -> drop_table(table) end)
      Postgrex.query!(conn, "INSERT INTO #{table} VALUES (1, 0)", [])

      update = "UPDATE #{table} SET n = n + 1 WHERE id = $1"

      for _ <- 1..3 do
        assert {:ok, {:ok, %{num_rows: 1}}} =
                 Postgrex.transaction(conn, fn tx -> Adapter.execute(tx, update, [1], []) end)
      end

      assert %{rows: [[3]]} = Postgrex.query!(conn, "SELECT n FROM #{table}", [])
    end
  end

  defp drop_table(table) do
    {:ok, conn} = Postgrex.start_link(ConnectionOptions.options())
    Postgrex.query!(conn, "DROP TABLE IF EXISTS #{table}", [])
    GenServer.stop(conn)
  end

  # Runs `fun` and returns its result with the payload of every socket send
  # made meanwhile by any process.
  defp socket_frames(fun) do
    tracer = spawn_link(fn -> collect_frames([]) end)
    patterns = [{:gen_tcp, :send, 2}, {:ssl, :send, 2}]

    Enum.each(patterns, fn {module, _function, _arity} = mfa ->
      Code.ensure_loaded!(module)
      1 = :erlang.trace_pattern(mfa, true, [:global])
    end)

    :erlang.trace(:all, true, [:call, {:tracer, tracer}])

    try do
      fun.()
    after
      :erlang.trace(:all, false, [:call])
      ref = :erlang.trace_delivered(:all)

      receive do
        {:trace_delivered, :all, ^ref} -> :ok
      end

      Enum.each(patterns, &:erlang.trace_pattern(&1, false, [:global]))
    end
    |> then(fn result ->
      send(tracer, {:frames, self()})

      receive do
        {:frames, frames} -> {result, frames}
      end
    end)
  end

  defp collect_frames(frames) do
    receive do
      {:trace, _pid, :call, {_module, :send, [_socket, data]}} ->
        collect_frames([IO.iodata_to_binary(data) | frames])

      {:frames, caller} ->
        send(caller, {:frames, Enum.reverse(frames)})
    end
  end
end
