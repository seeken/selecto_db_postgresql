defmodule SelectoDBPostgreSQL.ReadEndingIntegrationTest do
  # R72: how a read in a host transaction ends, with the ending made to fail.
  # A proxy rewrites the savepoint's name in the statement it passes on, so the
  # server itself answers 3B001 and the transaction really is in that state.
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  @moduletag :postgres

  defmodule Proxy do
    def start(host, port) do
      {:ok, listen} =
        :gen_tcp.listen(0, [
          :binary,
          packet: :raw,
          active: false,
          reuseaddr: true,
          ip: {127, 0, 0, 1}
        ])

      {:ok, local} = :inet.port(listen)
      {:ok, rules} = Agent.start_link(fn -> %{} end)
      spawn(fn -> accept(listen, host, port, rules) end)
      {local, rules}
    end

    # `{statement prefix => how many to break}`
    def break(rules, map), do: Agent.update(rules, fn _ -> map end)

    defp accept(listen, host, port, rules) do
      with {:ok, client} <- :gen_tcp.accept(listen),
           {:ok, server} <- :gen_tcp.connect(host, port, [:binary, packet: :raw, active: false]) do
        spawn(fn -> pump(server, client, fn data -> data end) end)
        spawn(fn -> pump(client, server, &rewrite(&1, rules)) end)
        accept(listen, host, port, rules)
      end
    end

    defp pump(from, to, fun) do
      case :gen_tcp.recv(from, 0) do
        {:ok, data} ->
          :gen_tcp.send(to, fun.(data))
          pump(from, to, fun)

        _closed ->
          :gen_tcp.close(to)
      end
    end

    defp rewrite(data, rules) do
      Agent.get_and_update(rules, fn map ->
        Enum.reduce(map, {data, map}, fn {prefix, left}, {data, map} ->
          needle = prefix <> " selecto_read_"

          if left > 0 and :binary.match(data, needle) != :nomatch do
            broken = prefix <> Agent.get(:selecto_end_mode, & &1)
            {:binary.replace(data, needle, broken), Map.put(map, prefix, left - 1)}
          else
            {data, map}
          end
        end)
      end)
    end
  end

  setup do
    # A name the server cannot parse fails as an undo that cannot run.
    start_supervised!(%{
      id: :selecto_end_mode,
      start: {Agent, :start_link, [fn -> " 1electo_read_" end, [name: :selecto_end_mode]]}
    })

    opts = ConnectionOptions.options()
    {local, rules} = Proxy.start(~c"127.0.0.1", Keyword.fetch!(opts, :port))

    {:ok, direct} = Adapter.connect(Keyword.put(opts, :pool_size, 1))
    Process.unlink(direct)
    table = "selecto_end_#{System.unique_integer([:positive])}"
    {:ok, _} = Adapter.execute(direct, "CREATE TABLE public.#{table} (id int)", [], [])

    {:ok, c} =
      opts
      |> Keyword.merge(port: local, hostname: "127.0.0.1", pool_size: 1)
      |> Adapter.connect()

    Process.unlink(c)

    on_exit(fn ->
      {:ok, d} = Adapter.connect(Keyword.put(opts, :pool_size, 1))
      Adapter.execute(d, "DROP TABLE IF EXISTS public.#{table}", [], [])
      GenServer.stop(d)
    end)

    %{c: c, direct: direct, rules: rules, table: table}
  end

  defp stored(ctx) do
    {:ok, %{rows: [[n]]}} =
      Adapter.execute(ctx.direct, "SELECT count(*) FROM public.#{ctx.table}", [], [])

    n
  end

  defp insert(conn, ctx, id),
    do: Postgrex.query(conn, "INSERT INTO public.#{ctx.table} VALUES (#{id})", [])

  test "a failed RELEASE reaches the host, and ROLLBACK TO keeps its transaction", ctx do
    Proxy.break(ctx.rules, %{"RELEASE SAVEPOINT" => 1})

    assert {:ok, :committed} =
             Postgrex.transaction(ctx.c, fn t ->
               {:ok, _} = insert(t, ctx, 1)

               assert {:error, %Postgrex.Error{postgres: %{code: :syntax_error}}} =
                        Adapter.execute(t, "SELECT 1", [], selecto_read: true)

               {:ok, _} = insert(t, ctx, 2)
               :committed
             end)

    assert stored(ctx) == 2
  end

  test "a read that cannot be undone closes, and nothing reports a false OK", ctx do
    Proxy.break(ctx.rules, %{"RELEASE SAVEPOINT" => 9, "ROLLBACK TO SAVEPOINT" => 9})

    result =
      try do
        Postgrex.transaction(ctx.c, fn t ->
          {:ok, _} = insert(t, ctx, 1)
          assert {:error, _} = Adapter.execute(t, "SELECT 1", [], selecto_read: true)
          assert {:error, %DBConnection.ConnectionError{}} = insert(t, ctx, 2)
          :committed
        end)
      rescue
        error in DBConnection.ConnectionError -> {:error, error}
      end

    assert {:error, _} = result
    assert stored(ctx) == 0

    Proxy.break(ctx.rules, %{})
    assert {:ok, %{rows: [[1]]}} = wait_for_read(ctx.c, 40)
  end

  test "3B001 on the undo is a savepoint never taken: no close", ctx do
    Agent.update(:selecto_end_mode, fn _ -> " selecto_gone_" end)
    Proxy.break(ctx.rules, %{"RELEASE SAVEPOINT" => 9, "ROLLBACK TO SAVEPOINT" => 9})
    [[backend]] = Postgrex.query!(ctx.c, "SELECT pg_backend_pid()", []).rows

    assert {:error, _} =
             Postgrex.transaction(ctx.c, fn t ->
               assert {:error, _} = Adapter.execute(t, "SELECT 1", [], selecto_read: true)
               Postgrex.rollback(t, :host_ends_it)
             end)

    Proxy.break(ctx.rules, %{})
    assert [[^backend]] = Postgrex.query!(ctx.c, "SELECT pg_backend_pid()", []).rows
  end

  test "a cancel on a catalog read's ROLLBACK TO is retried, and keeps nothing aborted" do
    canceled = {:error, %Postgrex.Error{postgres: %{code: :query_canceled, pg_code: "57014"}}}
    {:ok, left} = Agent.start_link(fn -> 2 end)
    {:ok, sent} = Agent.start_link(fn -> [] end)

    execute = fn statement, _params ->
      text = SelectoDBPostgreSQL.SessionGuard.control_sql(statement) || statement
      Agent.update(sent, &[text | &1])

      if String.starts_with?(text, "ROLLBACK TO") and
           Agent.get_and_update(left, &{&1 > 0, &1 - 1}),
         do: canceled,
         else: {:ok, %{rows: []}}
    end

    assert :ok =
             SelectoDBPostgreSQL.SessionGuard.undo_savepoint(
               execute,
               "selecto_path_0000000000000000"
             )

    assert length(Enum.filter(Agent.get(sent, & &1), &String.starts_with?(&1, "ROLLBACK TO"))) ==
             3

    assert Enum.any?(Agent.get(sent, & &1), &String.starts_with?(&1, "RELEASE"))
  end

  test "on a pid in a text BEGIN the failure is returned, not raised", ctx do
    {:ok, _} = Adapter.execute(ctx.c, "BEGIN", [], [])
    Proxy.break(ctx.rules, %{"RELEASE SAVEPOINT" => 9, "ROLLBACK TO SAVEPOINT" => 9})
    assert {:error, _} = Adapter.execute(ctx.c, "SELECT 1", [], selecto_read: true)
    Proxy.break(ctx.rules, %{})
    assert {:ok, %{rows: [[1]]}} = wait_for_read(ctx.c, 40)
  end

  test "under a host search_path too", ctx do
    {:ok, _} = Adapter.execute(ctx.c, "SET search_path = pg_catalog, public", [], [])
    Proxy.break(ctx.rules, %{"RELEASE SAVEPOINT" => 1})

    assert {:ok, :committed} =
             Postgrex.transaction(ctx.c, fn t ->
               assert {:error, _} = Adapter.execute(t, "SELECT 1", [], selecto_read: true)
               {:ok, _} = insert(t, ctx, 3)

               assert [["pg_catalog, public"]] =
                        Postgrex.query!(t, "SHOW search_path", []).rows

               :committed
             end)

    assert stored(ctx) == 1
  end

  defp wait_for_read(c, 0), do: Adapter.execute(c, "SELECT 1", [], selecto_read: true)

  defp wait_for_read(c, tries) do
    case Adapter.execute(c, "SELECT 1", [], selecto_read: true) do
      {:ok, _} = ok ->
        ok

      _reconnecting ->
        Process.sleep(50)
        wait_for_read(c, tries - 1)
    end
  end
end
