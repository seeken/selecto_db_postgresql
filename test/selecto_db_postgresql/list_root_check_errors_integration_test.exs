defmodule SelectoDBPostgreSQL.ListRootCheckErrorsIntegrationTest.Proxy do
  @moduledoc false
  # A TCP proxy in front of PostgreSQL that rewrites the Nth statement matching
  # a pattern into one that cancels itself on the server (57014), so the server
  # sees exactly a cancel landing on that statement.

  def start(target_port) do
    {:ok, listen} =
      :gen_tcp.listen(0, [
        :binary,
        packet: :raw,
        active: false,
        reuseaddr: true,
        ip: {127, 0, 0, 1}
      ])

    {:ok, port} = :inet.port(listen)
    {:ok, agent} = Agent.start(fn -> %{rule: nil, seen: 0, hits: 0} end)
    spawn(fn -> accept(listen, target_port, agent) end)
    {port, agent}
  end

  def arm(agent, regex, nth),
    do: Agent.update(agent, &%{&1 | rule: {regex, nth}, seen: 0, hits: 0})

  def disarm(agent), do: Agent.get_and_update(agent, &{&1.hits, %{&1 | rule: nil}})

  defp accept(listen, target_port, agent) do
    with {:ok, client} <- :gen_tcp.accept(listen),
         {:ok, server} <-
           :gen_tcp.connect({127, 0, 0, 1}, target_port, [:binary, packet: :raw, active: false]) do
      spawn(fn -> pipe(server, client) end)
      spawn(fn -> startup(client, server, agent) end)
      accept(listen, target_port, agent)
    end
  end

  defp pipe(from, to) do
    case :gen_tcp.recv(from, 0) do
      {:ok, data} ->
        if :gen_tcp.send(to, data) == :ok, do: pipe(from, to), else: :gen_tcp.close(from)

      _ ->
        :gen_tcp.close(to)
    end
  end

  defp startup(client, server, agent) do
    with {:ok, <<len::32>>} <- :gen_tcp.recv(client, 4),
         {:ok, body} <- :gen_tcp.recv(client, len - 4) do
      :gen_tcp.send(server, <<len::32>> <> body)

      case body do
        <<80_877_103::32>> -> startup(client, server, agent)
        _ -> loop(client, server, agent)
      end
    else
      _ -> :gen_tcp.close(server)
    end
  end

  defp loop(client, server, agent) do
    with {:ok, <<type, len::32>>} <- :gen_tcp.recv(client, 5),
         {:ok, body} <- if(len > 4, do: :gen_tcp.recv(client, len - 4), else: {:ok, ""}) do
      :gen_tcp.send(server, rewrite(type, body, agent))
      loop(client, server, agent)
    else
      _ -> :gen_tcp.close(server)
    end
  end

  @cancel "SELECT pg_catalog.pg_cancel_backend(pg_catalog.pg_backend_pid()), pg_catalog.pg_sleep(5)"
  @cancel_param "SELECT n FROM pg_catalog.unnest($1::pg_catalog.text[]) AS n " <>
                  "WHERE pg_catalog.pg_cancel_backend(pg_catalog.pg_backend_pid()) " <>
                  "AND (SELECT true FROM pg_catalog.pg_sleep(5))"

  defp rewrite(?Q, body, agent) do
    sql = binary_part(body, 0, byte_size(body) - 1)
    if hit?(agent, sql), do: frame(?Q, @cancel <> <<0>>), else: frame(?Q, body)
  end

  defp rewrite(?P, body, agent) do
    [name, rest] = :binary.split(body, <<0>>)
    [sql, tail] = :binary.split(rest, <<0>>)

    if hit?(agent, sql) do
      replacement = if String.contains?(sql, "$1"), do: @cancel_param, else: @cancel
      frame(?P, name <> <<0>> <> replacement <> <<0>> <> tail)
    else
      frame(?P, body)
    end
  end

  defp rewrite(type, body, _agent), do: frame(type, body)

  defp frame(type, body), do: <<type, byte_size(body) + 4::32>> <> body

  defp hit?(agent, sql) do
    Agent.get_and_update(agent, fn
      %{rule: {regex, nth}} = state ->
        if Regex.match?(regex, sql) do
          seen = state.seen + 1
          hit = seen == nth
          {hit, %{state | seen: seen, hits: state.hits + if(hit, do: 1, else: 0)}}
        else
          {false, state}
        end

      state ->
        {false, state}
    end)
  end
end

defmodule SelectoDBPostgreSQL.ListRootCheckErrorsIntegrationTest do
  # The list-root check (a root over a list named like a relation on the
  # search_path refuses) runs its own SAVEPOINT, lookup and RELEASE in a host
  # transaction. Whatever fails there comes back typed like the same error from
  # the read, never as a raise.
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.ListRootCheckErrorsIntegrationTest.Proxy
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  @moduletag :postgres
  @moduletag timeout: 120_000

  setup do
    opts = ConnectionOptions.options()
    {port, agent} = Proxy.start(Keyword.fetch!(opts, :port))

    {:ok, host} =
      opts
      |> Keyword.put(:hostname, "127.0.0.1")
      |> Keyword.put(:port, port)
      |> Postgrex.start_link()

    %{host: host, agent: agent, list: "lrc_#{System.unique_integer([:positive])}"}
  end

  defp list_root(ctx, conn) do
    %{
      name: ctx.list,
      schemas: %{},
      joins: %{},
      source: %{
        source_table: ctx.list,
        primary_key: :id,
        fields: [:id, :v],
        redact_fields: [],
        columns: %{id: %{type: :integer}, v: %{type: :string}},
        associations: %{}
      }
    }
    |> Selecto.configure(conn, adapter: Adapter, validate: false)
    |> Selecto.with_values([[1, "L"]], columns: ["id", "v"], as: ctx.list)
    |> Selecto.select(["v"])
  end

  defp code(%Selecto.Error{details: details}), do: details[:code]

  test "an aborted host transaction comes back typed", ctx do
    Postgrex.transaction(ctx.host, fn tx ->
      {:error, _} = Postgrex.query(tx, "SELECT 1/0", [])
      assert {:error, %Selecto.Error{} = error} = Selecto.execute(list_root(ctx, tx))
      assert code(error) == :in_failed_sql_transaction
      Postgrex.rollback(tx, :done)
    end)
  end

  test "a cancel on each of the check's statements comes back typed, or is undone", ctx do
    # A cancel on the SAVEPOINT or the lookup fails the read typed; one on the
    # savepoint's ROLLBACK TO or RELEASE is retried (SessionGuard.undo_savepoint/3)
    # and the read answers, the host's transaction intact.
    for {pattern, outcome} <- [
          {~r/\ASAVEPOINT selecto_path_/, :canceled},
          {~r/to_regclass\(pg_catalog\.quote_ident\(n\)\) IS NOT NULL/, :canceled},
          {~r/\AROLLBACK TO SAVEPOINT selecto_path_/, :answered},
          {~r/\ARELEASE SAVEPOINT selecto_path_/, :answered}
        ] do
      Postgrex.transaction(ctx.host, fn tx ->
        Proxy.arm(ctx.agent, pattern, 1)
        result = Selecto.execute(list_root(ctx, tx))
        assert Proxy.disarm(ctx.agent) == 1, inspect(pattern)

        case outcome do
          :canceled ->
            assert {:error, %Selecto.Error{} = error} = result, inspect(pattern)
            assert code(error) == :query_canceled, inspect(pattern)

          :answered ->
            assert {:ok, {[["L"]], _, _}} = result, inspect(pattern)
            assert {:ok, %{rows: [[1]]}} = Postgrex.query(tx, "SELECT 1", [])
        end

        Postgrex.rollback(tx, :done)
      end)
    end

    # Unarmed, the same read answers on the same connection.
    Postgrex.transaction(ctx.host, fn tx ->
      assert {:ok, {[["L"]], _, _}} = Selecto.execute(list_root(ctx, tx))
    end)
  end
end
