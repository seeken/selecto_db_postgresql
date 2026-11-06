defmodule SelectoDBPostgreSQL.UnnamedStatementTest do
  # R63: no statement of the adapter is named, whatever the configuration, so
  # nothing a statement reaches can DEALLOCATE and PREPARE it. A name left on
  # the connection, before or by a read, is never bound.
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  describe "unnamed/1" do
    test "drops a statement name, from the caller's options too" do
      assert Adapter.unnamed(timeout: 5) == [timeout: 5]
      assert Adapter.unnamed(cache_statement: "x") == []
    end
  end

  describe "live" do
    @describetag :postgres

    setup do
      opts = Keyword.put(ConnectionOptions.options(), :pool_size, 1)
      {:ok, c} = Adapter.connect(opts)
      Process.unlink(c)
      table = "selecto_unnamed_#{System.unique_integer([:positive])}"

      for sql <- [
            "CREATE TABLE public.#{table} (id int PRIMARY KEY, org text NOT NULL)",
            "INSERT INTO public.#{table} VALUES (1, 't1'), (2, 't2')"
          ],
          do: {:ok, _} = Adapter.execute(c, sql, [], [])

      on_exit(fn ->
        {:ok, d} = Adapter.connect(opts)
        Adapter.execute(d, "DROP TABLE IF EXISTS public.#{table}", [], [])
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
      {:ok, %{rows: rows}} =
        Adapter.execute(c, "SELECT name FROM pg_catalog.pg_prepared_statements", [], [])

      List.flatten(rows)
    end

    test "reads, streams and transactions leave no named statement", ctx do
      for _ <- 1..2 do
        assert {:ok, {[[1]], _, _}} = Selecto.execute(read(ctx.connection, ctx.table))
      end

      {:ok, stream} =
        Adapter.stream(ctx.connection, "SELECT id FROM public.#{ctx.table}", [],
          selecto_read: true
        )

      assert length(Enum.to_list(stream)) == 2
      assert prepared(ctx.connection) == []
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

      for sql <- ["COMMIT", "ROLLBACK", "BEGIN", "SAVEPOINT q", "SELECT 1; SELECT 2"] do
        assert {:error, %{details: %{reason: :sql_unqualifiable}}} =
                 Adapter.execute(c, sql, [], selecto_read: true)

        assert DBConnection.run(c, &DBConnection.status/1) == :idle
      end

      assert {:ok, %{rows: [[1]]}} = Adapter.execute(c, "SELECT 1;", [], selecto_read: true)
    end

    test "every name a statement could have, planted, stays unbound", ctx do
      # On one pooled connection, so the names outlive each checkout.
      for slot <- 0..255 do
        {:ok, _} =
          Adapter.execute(
            ctx.connection,
            "PREPARE selecto_#{slot} AS SELECT 2, 't2'::text",
            [],
            []
          )
      end

      for _ <- 1..3 do
        assert {:ok, {[[1]], _, _}} = Selecto.execute(read(ctx.connection, ctx.table))
      end

      assert length(prepared(ctx.connection)) == 256
    end
  end
end
