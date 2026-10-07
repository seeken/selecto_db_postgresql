defmodule SelectoDBPostgreSQL.ServerVersionRoundTripIntegrationTest do
  @moduledoc """
  The adapter reads the server's major version from the `server_version`
  Postgrex keeps for each connection, so configure's automatic
  `rollup_sort_fix` and every write-capability check cost no round trip.

  Round trips are counted by tracing the socket sends Postgrex makes
  (`:gen_tcp.send/2` and `:ssl.send/2`) in every process.
  """

  use ExUnit.Case, async: false

  alias Selecto.Write.Command
  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  describe "parse_server_version_major/1" do
    test "reads the leading integer of release, pre-release and packaged versions" do
      assert Adapter.parse_server_version_major("18.0") == {:ok, 18}
      assert Adapter.parse_server_version_major("17.6") == {:ok, 17}
      assert Adapter.parse_server_version_major("16.4 (Debian 16.4-1.pgdg120+2)") == {:ok, 16}
      assert Adapter.parse_server_version_major("19devel") == {:ok, 19}
      assert Adapter.parse_server_version_major("18beta1") == {:ok, 18}
      assert Adapter.parse_server_version_major("18rc1") == {:ok, 18}
      assert Adapter.parse_server_version_major("9.6.24") == {:ok, 9}
    end

    test "leaves anything else to the server_version_num query" do
      assert Adapter.parse_server_version_major(nil) == :unknown
      assert Adapter.parse_server_version_major("") == :unknown
      assert Adapter.parse_server_version_major("PostgreSQL 16") == :unknown
      assert Adapter.parse_server_version_major("0.1") == :unknown
      assert Adapter.parse_server_version_major("-16") == :unknown
    end
  end

  describe "live connections" do
    @describetag :postgres

    setup do
      {:ok, conn} = Postgrex.start_link(ConnectionOptions.options())
      Process.unlink(conn)
      on_exit(fn -> if Process.alive?(conn), do: GenServer.stop(conn) end)

      %{rows: [[version_num]]} = Postgrex.query!(conn, "show server_version_num", [])
      major = div(String.to_integer(version_num), 10_000)

      %{conn: conn, major: major}
    end

    test "the counter sees a server_version_num query", %{conn: conn} do
      {_result, frames} =
        socket_frames(fn -> Postgrex.query!(conn, "show server_version_num", []) end)

      assert Enum.count(frames, &version_query?/1) == 1
    end

    test "server_version_major agrees with server_version_num without a round trip",
         %{conn: conn, major: major} do
      name = :"selecto_version_named_#{System.unique_integer([:positive])}"
      {:ok, named} = Postgrex.start_link(Keyword.put(ConnectionOptions.options(), :name, name))
      Process.unlink(named)
      on_exit(fn -> if Process.alive?(named), do: GenServer.stop(named) end)

      pool = start_pool!()

      # A named connection answers once it has connected.
      assert {:ok, ^major} = Adapter.server_version_major(name)

      for connection <- [conn, name, pool] do
        assert {{:ok, ^major}, []} =
                 socket_frames(fn -> Adapter.server_version_major(connection) end)
      end

      assert {:ok, {{:ok, ^major}, []}} =
               Postgrex.transaction(conn, fn transactional ->
                 socket_frames(fn -> Adapter.server_version_major(transactional) end)
               end)
    end

    test "configure resolves rollup_sort_fix without a round trip",
         %{conn: conn, major: major} do
      pool = start_pool!()

      for connection <- [conn, pool] do
        {selecto, frames} =
          socket_frames(fn -> Selecto.configure(domain(), connection, adapter: Adapter) end)

        assert frames == []
        assert selecto.config.rollup_sort_fix == major < 18
      end
    end

    test "write capabilities report the server's version without a round trip",
         %{conn: conn, major: major} do
      {capabilities, frames} = socket_frames(fn -> Adapter.write_capabilities(conn) end)

      assert frames == []
      assert capabilities.server_major == major
      assert capabilities.merge == major >= 15
      assert capabilities.merge_returning == major >= 17
    end

    test "an executed write sends only its own statements", %{conn: conn} do
      table = create_items!(conn)
      selecto = Selecto.configure(domain(table), conn, adapter: Adapter)
      command = insert!(table, 1)

      {result, frames} = socket_frames(fn -> Selecto.Write.execute_unsafe(selecto, command) end)

      assert {:ok, %Selecto.Write.Result{affected_rows: 1}} = result
      refute Enum.any?(frames, &version_query?/1)
      assert Enum.count(frames, &String.contains?(&1, "INSERT INTO")) == 1
    end

    test "a prepared write checks capabilities in its transaction without a round trip",
         %{conn: conn} do
      table = create_items!(conn)
      command = insert!(table, 2)

      {result, frames} =
        socket_frames(fn ->
          Adapter.execute_prepared_write_unsafe(conn, fn _loader -> {:ok, command, %{}} end)
        end)

      assert {:ok, %Selecto.Write.Result{affected_rows: 1}} = result
      refute Enum.any?(frames, &version_query?/1)
    end
  end

  defp start_pool! do
    {:ok, pool_ref} =
      Selecto.ConnectionPool.start_pool(ConnectionOptions.options(),
        adapter: Adapter,
        pool_size: 1,
        max_overflow: 0
      )

    on_exit(fn -> Selecto.ConnectionPool.stop_pool(pool_ref) end)
    pool = {:pool, pool_ref}

    # Wait for the pooled connection so its startup is not counted.
    {:ok, _result} = Adapter.execute(pool, "SELECT 1", [], [])
    pool
  end

  defp create_items!(conn) do
    table = "selecto_version_items_#{System.unique_integer([:positive])}"
    Postgrex.query!(conn, "CREATE TEMP TABLE #{table} (id integer PRIMARY KEY, name text)", [])
    table
  end

  defp insert!(table, id) do
    {:ok, command} =
      Command.new(%{
        operation: :insert,
        relation: String.to_atom(table),
        assignments: [
          %{field: :id, value: {:literal, id}},
          %{field: :name, value: {:literal, "item #{id}"}}
        ],
        expected_cardinality: {:exactly, 1}
      })

    command
  end

  defp version_query?(frame), do: String.contains?(frame, "server_version")

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

  defp domain(table \\ "pg_class") do
    %{
      name: "Server version",
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
