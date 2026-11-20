Code.require_file("../support/wire_trace.exs", __DIR__)

defmodule SelectoDBPostgreSQL.ReadShapesIntegrationTest do
  # The user rulings of 2026-11-20 (R63 withdrawn, R55 narrowed):
  #
  # - a generated read with no deadline asked for is one autocommit statement:
  #   no transaction, no READ ONLY, no statement_timeout;
  # - hand-built text keeps its READ ONLY transaction, which refuses a write;
  # - a deadline that was asked for is a statement_timeout local to the read's
  #   own transaction, so the session's value is unchanged afterwards;
  # - client_encoding comes from the driver's ParameterStatus, and a session
  #   that is not UTF8 is still refused.
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions
  alias SelectoDBPostgreSQL.WireTrace

  @moduletag :postgres

  # What Selecto's executor sends for a generated read: with `timeout:` from the
  # caller, or its own default marked as such.
  @generated [selecto_read: true, selecto_generated: true]
  @default @generated ++ [timeout: 30_000, selecto_default_timeout: true]

  setup do
    opts = Keyword.put(ConnectionOptions.options(), :pool_size, 1)
    {:ok, c} = Adapter.connect(opts)
    Process.unlink(c)
    {:ok, other} = Postgrex.start_link(opts)
    Process.unlink(other)
    table = "selecto_shapes_#{System.unique_integer([:positive])}"

    for sql <- [
          "CREATE TABLE public.#{table} (id int PRIMARY KEY, org text NOT NULL)",
          "INSERT INTO public.#{table} VALUES (1, 't1'), (2, 't2')",
          # The host's own function, which writes.
          "CREATE FUNCTION public.#{table}_touch() RETURNS int LANGUAGE sql VOLATILE AS " <>
            "'INSERT INTO public.#{table} VALUES (100 + (SELECT count(*) FROM public.#{table}), ''w'') RETURNING id'"
        ],
        do: Postgrex.query!(other, sql, [])

    on_exit(fn ->
      for pid <- [c, other], Process.alive?(pid), do: GenServer.stop(pid)
      {:ok, d} = Postgrex.start_link(opts)
      Postgrex.query!(d, "DROP FUNCTION IF EXISTS public.#{table}_touch()", [])
      Postgrex.query!(d, "DROP TABLE IF EXISTS public.#{table}", [])
      GenServer.stop(d)
    end)

    %{c: c, other: other, table: table}
  end

  defp status(c), do: DBConnection.run(c, &DBConnection.status/1)

  defp written(other, table),
    do: count(other, "SELECT count(*) FROM public.#{table} WHERE org = 'w'")

  defp count(conn, sql), do: Postgrex.query!(conn, sql, []).rows |> hd() |> hd()

  defp session(c),
    do:
      Postgrex.query!(
        c,
        "SELECT current_setting('statement_timeout'), current_setting('transaction_read_only')",
        []
      ).rows

  defp put_setting(key, value) do
    previous = Application.fetch_env(:selecto_db_postgresql, key)
    Application.put_env(:selecto_db_postgresql, key, value)

    on_exit(fn ->
      case previous do
        {:ok, old} -> Application.put_env(:selecto_db_postgresql, key, old)
        :error -> Application.delete_env(:selecto_db_postgresql, key)
      end
    end)
  end

  defp domain(table) do
    %{
      name: "S",
      source: %{
        source_table: table,
        primary_key: :id,
        tenant_field: :org,
        fields: [:id, :org],
        redact_fields: [],
        columns: %{id: %{type: :integer}, org: %{type: :string}},
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  defp read(c, table) do
    domain(table)
    |> Selecto.configure(c, adapter: Adapter, validate: false)
    |> Selecto.with_tenant("t1")
    |> Selecto.select(["id"])
  end

  describe "a generated read with no deadline asked for" do
    test "is the statement alone: no transaction, no READ ONLY, no timeout", ctx do
      sql =
        "SELECT current_setting('transaction_read_only'), current_setting('statement_timeout'), " <>
          "pg_catalog.pg_current_xact_id_if_assigned() IS NULL"

      for opts <- [@generated, @default] do
        {result, statements} = WireTrace.calls(fn -> Adapter.execute(ctx.c, sql, [], opts) end)
        assert {:ok, %{rows: [["off", "0", true]]}} = result
        # Nothing before it and nothing after it: no BEGIN, no ROLLBACK.
        assert statements == [sql]
        assert status(ctx.c) == :idle
      end

      # One round trip once the statement is prepared.
      {_result, frames} = WireTrace.frames(fn -> Adapter.execute(ctx.c, sql, [], @default) end)
      assert length(frames) == 1
    end

    test "through Selecto.execute/2 it is one statement and one round trip", ctx do
      assert {:ok, {[[1]], _, _}} = Selecto.execute(read(ctx.c, ctx.table))

      {result, statements} = WireTrace.calls(fn -> Selecto.execute(read(ctx.c, ctx.table)) end)
      assert {:ok, {[[1]], _, _}} = result
      assert [statement] = statements
      assert statement =~ ctx.table

      {_result, frames} = WireTrace.frames(fn -> Selecto.execute(read(ctx.c, ctx.table)) end)
      assert length(frames) == 1
    end

    test "the server saw no transaction around it: a write the host's function makes commits",
         ctx do
      # The documented consequence of narrowing R55. Under BEGIN READ ONLY this
      # refused with 25006; under BEGIN ... ROLLBACK it would be undone.
      assert {:ok, %{rows: [[102]]}} =
               Adapter.execute(ctx.c, "SELECT public.#{ctx.table}_touch()", [], @default)

      assert status(ctx.c) == :idle
      assert written(ctx.other, ctx.table) == 1
    end

    test "inside the host's transaction it is the host's own statement, alone", ctx do
      sql = "SELECT id FROM public.#{ctx.table} WHERE org = $1"

      {result, statements} =
        WireTrace.calls(fn ->
          Postgrex.transaction(ctx.c, fn tx -> Adapter.execute(tx, sql, ["t1"], @default) end)
        end)

      assert {:ok, {:ok, %{rows: [[1]]}}} = result
      assert statements == [sql]
    end

    test "read_deadline: :always sends the default as a server deadline again", ctx do
      put_setting(:read_deadline, :always)

      sql =
        "SELECT current_setting('transaction_read_only'), current_setting('statement_timeout')"

      {result, statements} = WireTrace.calls(fn -> Adapter.execute(ctx.c, sql, [], @default) end)
      assert {:ok, %{rows: [["on", timeout]]}} = result
      assert timeout =~ ~r/\A(\d+ms|15s)\z/
      assert ["BEGIN READ ONLY; SELECT pg_catalog.set_config" <> _, ^sql, "ROLLBACK"] = statements
      assert session(ctx.c) == [["0", "off"]]
    end

    test "with no server deadline the driver's timeout ends it and the pool recovers", ctx do
      started = System.monotonic_time(:millisecond)

      assert {:error, _reason} =
               Adapter.execute(
                 ctx.c,
                 "SELECT pg_catalog.pg_sleep(5)",
                 [],
                 @generated ++ [timeout: 300, selecto_default_timeout: true]
               )

      assert System.monotonic_time(:millisecond) - started < 3_000
      assert {:ok, %{rows: [[1]]}} = Adapter.execute(ctx.c, "SELECT 1", [], @default)
      assert session(ctx.c) == [["0", "off"]]
    end
  end

  describe "hand-built text" do
    test "keeps its READ ONLY transaction and refuses a write (25006)", ctx do
      sql = "SELECT public.#{ctx.table}_touch()"

      {result, statements} =
        WireTrace.calls(fn -> Adapter.execute(ctx.c, sql, [], selecto_read: true) end)

      assert {:error, %Selecto.Error{details: %{reason: :read_only_read}}} = result
      assert statements == ["BEGIN READ ONLY", sql, "ROLLBACK"]
      assert status(ctx.c) == :idle
      assert written(ctx.other, ctx.table) == 0

      # With Selecto's default deadline marked, too.
      assert {:error, %Selecto.Error{details: %{reason: :read_only_read}}} =
               Adapter.execute(
                 ctx.c,
                 sql,
                 [],
                 selecto_read: true,
                 timeout: 30_000,
                 selecto_default_timeout: true
               )

      assert written(ctx.other, ctx.table) == 0
    end

    test "a data-modifying CTE is refused by the server, not committed", ctx do
      sql =
        "WITH gone AS (DELETE FROM public.#{ctx.table} RETURNING id) SELECT count(*) FROM gone"

      assert {:error, _refused} = Adapter.execute(ctx.c, sql, [], selecto_read: true)
      assert status(ctx.c) == :idle
      assert count(ctx.other, "SELECT count(*) FROM public.#{ctx.table}") == 2
    end

    test "a read reads, in READ ONLY", ctx do
      assert {:ok, %{rows: [["on"]]}} =
               Adapter.execute(ctx.c, "SELECT current_setting('transaction_read_only')", [],
                 selecto_read: true
               )

      assert session(ctx.c) == [["0", "off"]]
    end
  end

  describe "a deadline that was asked for" do
    test "is enforced by the server and leaves the session's timeout at 0", ctx do
      assert session(ctx.c) == [["0", "off"]]

      {result, statements} =
        WireTrace.calls(fn ->
          Adapter.execute(
            ctx.c,
            "SELECT pg_catalog.pg_sleep(2)",
            [],
            @generated ++ [timeout: 100]
          )
        end)

      assert {:error, %Postgrex.Error{postgres: %{code: :query_canceled}}} = result

      assert ["BEGIN READ ONLY; SELECT pg_catalog.set_config" <> setup, _sleep, "ROLLBACK"] =
               statements

      assert String.ends_with?(setup, "AS selecto_guard_deadline")
      refute setup =~ "client_encoding"

      # The same connection (pool of one, same backend), idle, timeout untouched.
      assert status(ctx.c) == :idle
      assert session(ctx.c) == [["0", "off"]]
    end

    test "is local to the read: seen inside, gone after, and READ ONLY", ctx do
      pid = count(ctx.c, "SELECT pg_backend_pid()")

      assert {:ok, %{rows: [["on", timeout, ^pid]]}} =
               Adapter.execute(
                 ctx.c,
                 "SELECT current_setting('transaction_read_only'), " <>
                   "current_setting('statement_timeout'), pg_backend_pid()",
                 [],
                 @generated ++ [timeout: 1_000]
               )

      assert timeout =~ ~r/\A(\d+ms|1s)\z/
      assert session(ctx.c) == [["0", "off"]]

      # A generated read that would write is refused inside that transaction.
      assert {:error, %Selecto.Error{details: %{reason: :read_only_read}}} =
               Adapter.execute(
                 ctx.c,
                 "SELECT public.#{ctx.table}_touch()",
                 [],
                 @generated ++ [timeout: 1_000]
               )

      assert written(ctx.other, ctx.table) == 0
    end

    test "inside the host's transaction it is set, enforced and put back", ctx do
      assert {:ok, {"0", "0"}} =
               Postgrex.transaction(ctx.c, fn tx ->
                 before = count(tx, "SHOW statement_timeout")

                 assert {:ok, %{rows: [[inside]]}} =
                          Adapter.execute(
                            tx,
                            "SELECT current_setting('statement_timeout')",
                            [],
                            @generated ++ [timeout: 1_000]
                          )

                 assert inside =~ ~r/\A(\d+ms|1s)\z/
                 {before, count(tx, "SHOW statement_timeout")}
               end)

      assert session(ctx.c) == [["0", "off"]]
    end
  end

  describe "a session that is not UTF8" do
    test "refuses every read shape before anything is sent", ctx do
      Postgrex.query!(ctx.c, "SET client_encoding = 'LATIN1'", [])
      sql = "SELECT $1::text"

      for opts <- [
            @generated,
            @default,
            @generated ++ [timeout: 1_000],
            [selecto_read: true],
            [selecto_read: true, timeout: 1_000]
          ] do
        {result, statements} =
          WireTrace.calls(fn -> Adapter.execute(ctx.c, sql, ["café"], opts) end)

        assert {:error,
                %Selecto.Error{
                  type: :query_enforcement,
                  details: %{code: :query_rule_not_evaluable, reason: :client_encoding}
                }} = result

        assert statements == []
        assert status(ctx.c) == :idle
      end

      # In the host's transaction, which goes on.
      assert {:ok, :refused} =
               Postgrex.transaction(ctx.c, fn tx ->
                 assert {:error, %Selecto.Error{details: %{reason: :client_encoding}}} =
                          Adapter.execute(tx, sql, ["café"], @default)

                 assert %{rows: [[1]]} = Postgrex.query!(tx, "SELECT 1", [])
                 :refused
               end)

      # A stream, at the call.
      assert {:error, %Selecto.Error{details: %{reason: :client_encoding}}} =
               Adapter.stream(ctx.c, "SELECT 1", [], selecto_read: true)

      # Through Selecto.execute/2.
      assert {:error, %Selecto.Error{details: %{reason: :client_encoding}}} =
               Selecto.execute(read(ctx.c, ctx.table))

      # The driver follows the session: back to UTF8 (or its alias), reads run.
      for name <- ["UNICODE", "UTF8"] do
        Postgrex.query!(ctx.c, "SET client_encoding = '#{name}'", [])
        assert {:ok, %{rows: [["café"]]}} = Adapter.execute(ctx.c, sql, ["café"], @default)
      end
    end

    test "a connection default of LATIN1 is refused on the first read" do
      opts =
        ConnectionOptions.options()
        |> Keyword.put(:pool_size, 1)
        |> Keyword.put(:parameters, client_encoding: "LATIN1")

      {:ok, c} = Postgrex.start_link(opts)
      Process.unlink(c)
      on_exit(fn -> if Process.alive?(c), do: GenServer.stop(c) end)

      assert {:error, %Selecto.Error{details: %{reason: :client_encoding}}} =
               Adapter.execute(c, "SELECT 1", [], @default)
    end
  end
end
