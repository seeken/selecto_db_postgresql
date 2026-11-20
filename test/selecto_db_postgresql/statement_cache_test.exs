Code.require_file("../support/wire_trace.exs", __DIR__)

defmodule SelectoDBPostgreSQL.StatementCacheTest do
  # The `:statement_cache` setting (on by default; R63 was withdrawn by the
  # user ruling of 2026-11-20): a statement is prepared once per connection
  # under a hash-slot name, so a repeated statement costs one round trip
  # (Bind/Execute) instead of two (Parse/Describe, then Bind/Execute). A name
  # the server lost or whose result type changed is prepared again, and
  # `statement_cache: false` is the opt-out for transaction-mode poolers.
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions
  alias SelectoDBPostgreSQL.WireTrace
  alias SelectoDBPostgreSQL.WriteParameter

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

  describe "unnamed/1" do
    test "drops a statement name, from the caller's options too" do
      assert Adapter.unnamed(timeout: 5) == [timeout: 5]
      assert Adapter.unnamed(cache_statement: "x") == []
    end
  end

  describe "statement_cache_opts/3" do
    test "is on (256 slots) by default" do
      Application.delete_env(:selecto_db_postgresql, :statement_cache)

      assert Adapter.statement_cache_opts("SELECT 1", [], timeout: 5) == [
               cache_statement: "selecto_#{:erlang.phash2("SELECT 1", 256)}",
               timeout: 5
             ]
    end

    test "leaves a statement unnamed when the cache is off (false or 0)" do
      for off <- [false, 0, nil, -1, :yes] do
        Application.put_env(:selecto_db_postgresql, :statement_cache, off)
        assert Adapter.statement_cache_opts("SELECT 1", [], []) == []
        assert Adapter.statement_cache_opts("SELECT 1", [], cache_statement: "q") == []
      end
    end

    test "names a statement by a hash slot of its SQL" do
      Application.put_env(:selecto_db_postgresql, :statement_cache, 1024)
      name = "selecto_#{:erlang.phash2("SELECT 1", 1024)}"
      assert Adapter.statement_cache_opts("SELECT 1", [], []) == [cache_statement: name]

      Application.put_env(:selecto_db_postgresql, :statement_cache, 1)

      assert Adapter.statement_cache_opts("SELECT 1", [], []) ==
               Adapter.statement_cache_opts("SELECT 2", [], [])
    end

    test "keeps commented, unprepared and per-call described statements unnamed" do
      Application.put_env(:selecto_db_postgresql, :statement_cache, true)

      assert Adapter.statement_cache_opts("SELECT 1", [], comment: "x") == [comment: "x"]
      assert Adapter.statement_cache_opts("SELECT 1", [], prepared: false) == [prepared: false]

      marked = [%WriteParameter{field: :id, value: "x", tenant?: false}]
      assert WriteParameter.marked?(marked)
      assert Adapter.statement_cache_opts("SELECT $1", marked, []) == []
    end
  end

  describe "live" do
    @describetag :postgres

    setup do
      opts = Keyword.put(ConnectionOptions.options(), :pool_size, 1)
      {:ok, c} = Adapter.connect(opts)
      Process.unlink(c)
      table = "selecto_cache_#{System.unique_integer([:positive])}"

      for sql <- [
            "CREATE TABLE public.#{table} (id int PRIMARY KEY, org text NOT NULL)",
            "INSERT INTO public.#{table} VALUES (1, 't1'), (2, 't2')"
          ],
          do: Postgrex.query!(c, sql, [])

      on_exit(fn ->
        if Process.alive?(c), do: GenServer.stop(c)
        {:ok, d} = Postgrex.start_link(opts)
        Postgrex.query!(d, "DROP TABLE IF EXISTS public.#{table}", [])
        GenServer.stop(d)
      end)

      %{connection: c, table: table}
    end

    defp domain(table) do
      %{
        name: "U",
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

    defp prepared(c) do
      Postgrex.query!(c, "SELECT name FROM pg_catalog.pg_prepared_statements", []).rows
      |> List.flatten()
    end

    test "by default a repeated read is one round trip on a named statement", ctx do
      Application.delete_env(:selecto_db_postgresql, :statement_cache)
      assert {:ok, {[[1]], _, _}} = Selecto.execute(read(ctx.connection, ctx.table))

      {result, frames} =
        WireTrace.frames(fn -> Selecto.execute(read(ctx.connection, ctx.table)) end)

      assert {:ok, {[[1]], _, _}} = result
      assert length(frames) == 1
      assert [name] = prepared(ctx.connection)
      assert name =~ ~r/\Aselecto_\d+\z/
    end

    test "the pooler opt-out: off, nothing is named and every read prepares again", ctx do
      for off <- [false, 0] do
        Application.put_env(:selecto_db_postgresql, :statement_cache, off)

        for _ <- 1..2 do
          {result, frames} =
            WireTrace.frames(fn -> Selecto.execute(read(ctx.connection, ctx.table)) end)

          assert {:ok, {[[1]], _, _}} = result
          # Parse/Describe, then Bind/Execute, each time (one send when named).
          assert length(frames) >= 2
        end

        {:ok, stream} =
          Adapter.stream(ctx.connection, "SELECT id FROM public.#{ctx.table}", [],
            selecto_read: true
          )

        assert length(Enum.to_list(stream)) == 2
        assert prepared(ctx.connection) == []
      end
    end

    test "statements sharing a slot replace each other and return their own rows", ctx do
      Application.put_env(:selecto_db_postgresql, :statement_cache, 1)

      for _ <- 1..2 do
        assert {:ok, %{rows: [[1]]}} = Adapter.execute(ctx.connection, "SELECT 1", [], [])
        assert {:ok, %{rows: [["two"]]}} = Adapter.execute(ctx.connection, "SELECT 'two'", [], [])
      end

      assert prepared(ctx.connection) == ["selecto_0"]
    end

    test "a statement whose result type changed under DDL is prepared again", ctx do
      c = ctx.connection
      sql = "SELECT * FROM public.#{ctx.table} WHERE id = $1"
      opts = [selecto_read: true, selecto_generated: true]

      assert {:ok, %{rows: [[1, "t1"]]}} = Adapter.execute(c, sql, [1], opts)
      assert [name] = prepared(c)

      Postgrex.query!(c, "ALTER TABLE public.#{ctx.table} ADD COLUMN n int DEFAULT 7", [])
      assert {:ok, %{rows: [[1, "t1", 7]]}} = Adapter.execute(c, sql, [1], opts)

      Postgrex.query!(c, "ALTER TABLE public.#{ctx.table} ALTER COLUMN n TYPE text", [])
      assert {:ok, %{rows: [[1, "t1", "7"]]}} = Adapter.execute(c, sql, [1], opts)

      # Still named, and one round trip again.
      {result, frames} = WireTrace.frames(fn -> Adapter.execute(c, sql, [1], opts) end)
      assert {:ok, %{rows: [[1, "t1", "7"]]}} = result
      assert length(frames) == 1
      assert prepared(c) == [name]
    end

    test "a statement the server dropped (26000) is prepared again", ctx do
      c = ctx.connection

      for discard <- ["DEALLOCATE ALL", "DISCARD ALL"] do
        assert {:ok, {[[1]], _, _}} = Selecto.execute(read(c, ctx.table))
        assert [_name] = prepared(c)

        Postgrex.query!(c, discard, [])
        assert prepared(c) == []

        {result, statements} = WireTrace.calls(fn -> Selecto.execute(read(c, ctx.table)) end)
        assert {:ok, {[[1]], _, _}} = result
        # The failed Bind, then the statement again.
        assert [statement, statement] = statements
        assert [_name] = prepared(c)
      end
    end

    test "inside a transaction an invalidated statement fails as the server said", ctx do
      c = ctx.connection
      sql = "SELECT id FROM public.#{ctx.table} WHERE id = $1"
      assert {:ok, %{rows: [[1]]}} = Adapter.execute(c, sql, [1], [])
      Postgrex.query!(c, "DEALLOCATE ALL", [])

      assert {:error, :rollback} =
               Postgrex.transaction(c, fn tx ->
                 assert {:error, %Postgrex.Error{postgres: %{pg_code: "26000"}}} =
                          Adapter.execute(tx, sql, [1], [])
               end)

      assert {:ok, %{rows: [[1]]}} = Adapter.execute(c, sql, [1], [])
    end

    test "a name the session already holds is never bound: the read runs unnamed", ctx do
      c = ctx.connection

      # On one pooled connection, so the names outlive each checkout.
      for slot <- 0..255 do
        Postgrex.query!(c, "PREPARE selecto_#{slot} AS SELECT 2, 't2'::text", [])
      end

      for _ <- 1..3 do
        assert {:ok, {[[1]], _, _}} = Selecto.execute(read(c, ctx.table))
      end

      assert length(prepared(c)) == 256
    end

    test "writes run in a transaction on named statements", ctx do
      c = ctx.connection
      update = "UPDATE public.#{ctx.table} SET org = org WHERE id = $1"

      for _ <- 1..3 do
        assert {:ok, {:ok, %{num_rows: 1}}} =
                 Postgrex.transaction(c, fn tx -> Adapter.execute(tx, update, [1], []) end)
      end

      assert [_name] = prepared(c)
    end

    test "text is never sent over the simple protocol", ctx do
      c = ctx.connection
      table = "public.#{ctx.table}"
      rows = fn -> elem(Adapter.execute(c, "SELECT count(*) FROM #{table}", [], []), 1).rows end

      for sql <- [
            "COMMIT; DELETE FROM #{table} WHERE id = 2",
            "COMMIT; DELETE FROM #{table} WHERE id = 2; BEGIN",
            "ROLLBACK; DELETE FROM #{table} WHERE id = 2",
            "BEGIN; DELETE FROM #{table} WHERE id = 2",
            "SAVEPOINT s; DELETE FROM #{table} WHERE id = 2"
          ] do
        assert {:error, _} = Adapter.execute(c, sql, [], selecto_read: true)
        assert {:error, _} = Adapter.execute(c, sql, [], [])
        assert rows.() == [[2]]
        assert DBConnection.run(c, &DBConnection.status/1) == :idle
      end

      for sql <- ["COMMIT", "ROLLBACK", "BEGIN", "SAVEPOINT q"] do
        assert {:error, %{details: %{reason: :hand_built_not_a_read}}} =
                 Adapter.execute(c, sql, [], selecto_read: true)

        assert DBConnection.run(c, &DBConnection.status/1) == :idle
      end

      # One statement is the extended protocol's own rule.
      assert {:error, %Postgrex.Error{postgres: %{code: :syntax_error}}} =
               Adapter.execute(c, "SELECT 1; SELECT 2", [], selecto_read: true)

      assert DBConnection.run(c, &DBConnection.status/1) == :idle

      assert {:ok, %{rows: [[1]]}} = Adapter.execute(c, "SELECT 1;", [], selecto_read: true)
    end
  end
end
