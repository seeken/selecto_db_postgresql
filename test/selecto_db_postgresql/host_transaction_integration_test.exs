defmodule SelectoDBPostgreSQL.HostTransactionIntegrationTest do
  # A raw BEGIN on a pid or named connection: DBConnection.transaction/3 would
  # disconnect it, and the host's next statement would autocommit past its
  # ROLLBACK. A write joins under a savepoint; a stream refuses.
  use ExUnit.Case, async: false

  alias Selecto.Write.Command
  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.SessionGuard
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  @moduletag :postgres

  setup do
    opts = Keyword.put(ConnectionOptions.options(), :pool_size, 1)
    {:ok, c} = Adapter.connect(opts)
    Process.unlink(c)
    table = "selecto_host_#{System.unique_integer([:positive])}"

    for sql <- [
          "CREATE TABLE public.#{table} (id int PRIMARY KEY, org text, note text)",
          "INSERT INTO public.#{table} VALUES (1, 't1', 'n'), (2, 't1', 'n')"
        ],
        do: {:ok, _} = Adapter.execute(c, sql, [], [])

    on_exit(fn ->
      {:ok, d} = Adapter.connect(opts)
      Adapter.execute(d, "DROP TABLE IF EXISTS public.#{table}", [], [])
      GenServer.stop(d)
    end)

    %{connection: c, table: table}
  end

  defp sql!(c, sql) do
    {:ok, result} = Adapter.execute(c, sql, [], [])
    result.rows
  end

  defp write(c, table, id) do
    scope = {:eq, {:field, "org"}, {:literal, "t1"}}

    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: table,
        predicate: {:and, [{:eq, {:field, "id"}, {:literal, id}}, scope]},
        assignments: [%{field: "note", value: {:literal, "new"}}],
        expected_cardinality: :many,
        metadata: %{post_image: %{predicate: scope, key: ["id"]}}
      })

    Adapter.execute_write_unsafe(c, command, [])
  end

  test "a write through DBConnection.run joins, and the host's ROLLBACK takes it", ctx do
    c = ctx.connection

    DBConnection.run(c, fn conn ->
      Postgrex.query!(conn, "BEGIN", [])
      assert {:ok, %{affected_rows: 1}} = write(conn, ctx.table, 1)
      Postgrex.query!(conn, "ROLLBACK", [])
    end)

    assert sql!(c, "SELECT note FROM public.#{ctx.table} WHERE id = 1") == [["n"]]
  end

  test "a write on a pid finding a transaction it cannot prove the host's refuses", ctx do
    c = ctx.connection
    [[backend]] = sql!(c, "SELECT pg_backend_pid()")
    sql!(c, "BEGIN")

    assert {:error, %{details: %{reason: :unowned_transaction}}} = write(c, ctx.table, 1)

    sql!(c, "INSERT INTO public.#{ctx.table} VALUES (9, 't1', 'host')")
    assert sql!(c, "SELECT pg_backend_pid()") == [[backend]]
    sql!(c, "ROLLBACK")

    assert sql!(c, "SELECT count(*) FROM public.#{ctx.table} WHERE id = 9") == [[0]]
    assert sql!(c, "SELECT note FROM public.#{ctx.table} WHERE id = 1") == [["n"]]
  end

  test "an aborted host transaction refuses and keeps its connection", ctx do
    c = ctx.connection
    [[backend]] = sql!(c, "SELECT pg_backend_pid()")
    sql!(c, "BEGIN")
    sql!(c, "INSERT INTO public.#{ctx.table} VALUES (9, 't1', 'a')")
    assert {:error, _} = Adapter.execute(c, "SELECT 1/0", [], [])

    assert {:error, %{details: %{reason: :unowned_transaction}}} = write(c, ctx.table, 1)

    DBConnection.run(c, fn conn ->
      assert {:error, _} = write(conn, ctx.table, 1)
    end)

    sql!(c, "ROLLBACK")
    assert sql!(c, "SELECT pg_backend_pid()") == [[backend]]
    assert sql!(c, "SELECT count(*) FROM public.#{ctx.table} WHERE id = 9") == [[0]]
  end

  test "a write on a module that is no pool refuses, without an exit", ctx do
    defmodule NoPool do
      def query(_sql, _params), do: {:error, :no_pool}
    end

    scope = {:eq, {:field, "org"}, {:literal, "t1"}}

    {:ok, command} =
      Command.new(%{
        operation: :update,
        relation: ctx.table,
        predicate: {:and, [{:eq, {:field, "id"}, {:literal, 1}}, scope]},
        assignments: [%{field: "note", value: {:literal, "new"}}],
        expected_cardinality: :many,
        metadata: %{post_image: %{predicate: scope, key: ["id"]}}
      })

    assert {:error, %{type: :transaction_failed}} =
             Adapter.execute_write_unsafe(NoPool, command, [])
  end

  test "outside a transaction a write on a pid takes its own", ctx do
    assert {:ok, %{affected_rows: 1}} = write(ctx.connection, ctx.table, 1)

    assert sql!(ctx.connection, "SELECT note FROM public.#{ctx.table} WHERE id = 1") ==
             [["new"]]
  end

  test "a stream refuses inside the host's transaction, which goes on", ctx do
    c = ctx.connection
    [[backend]] = sql!(c, "SELECT pg_backend_pid()")
    sql!(c, "BEGIN")

    {:ok, stream} =
      Adapter.stream(c, "SELECT id FROM public.#{ctx.table}", [], selecto_read: true)

    assert_raise RuntimeError, ~r/cannot stream inside the host's transaction/, fn ->
      Enum.to_list(stream)
    end

    sql!(c, "INSERT INTO public.#{ctx.table} VALUES (9, 't1', 'host')")
    assert sql!(c, "SELECT pg_backend_pid()") == [[backend]]
    sql!(c, "ROLLBACK")

    assert sql!(c, "SELECT count(*) FROM public.#{ctx.table} WHERE id = 9") == [[0]]
  end

  test "a hand-built control mark carries no statement", ctx do
    forged = %SessionGuard.Control{sql: "COMMIT; DELETE FROM public.#{ctx.table}"}
    assert SessionGuard.control_sql(forged) == nil

    # Neither a struct by hand nor the old tuple reaches the simple protocol.
    for query <- [forged, {:selecto_control, "COMMIT"}], opts <- [[], [selecto_read: true]] do
      assert {:error, %{details: %{reason: :invalid_statement}}} =
               Adapter.execute(ctx.connection, query, [], opts)
    end

    assert sql!(ctx.connection, "SELECT count(*) FROM public.#{ctx.table}") == [[2]]
  end

  describe "client_encoding (R65)" do
    setup ctx do
      table = "selecto_enc_#{System.unique_integer([:positive])}"

      for sql <- [
            "CREATE TABLE public.#{table} (id int PRIMARY KEY, org text NOT NULL, note text)",
            "INSERT INTO public.#{table} VALUES (1, 'café', 'mine'), (2, 'cafÃ©', 'SECRET')",
            "CREATE FUNCTION public.#{table}_enc() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN " <>
              "IF NEW.note = 'plant' THEN " <>
              "PERFORM pg_catalog.set_config('client_encoding', 'LATIN1', false); END IF; " <>
              "RETURN NEW; END $$",
            "CREATE TRIGGER enc BEFORE UPDATE ON public.#{table} FOR EACH ROW " <>
              "EXECUTE FUNCTION public.#{table}_enc()"
          ],
          do: {:ok, _} = Adapter.execute(ctx.connection, sql, [], [])

      on_exit(fn ->
        {:ok, d} = Adapter.connect(Keyword.put(ConnectionOptions.options(), :pool_size, 1))
        Adapter.execute(d, "DROP TABLE IF EXISTS public.#{table}", [], [])
        Adapter.execute(d, "DROP FUNCTION IF EXISTS public.#{table}_enc()", [], [])
        GenServer.stop(d)
      end)

      %{enc: table}
    end

    defp enc_domain(table) do
      fields = [:id, :org, :note]

      %{
        name: "ENC",
        source: %{
          source_table: table,
          primary_key: :id,
          tenant_field: :org,
          fields: fields,
          redact_fields: [],
          columns: %{id: %{type: :integer}, org: %{type: :string}, note: %{type: :string}},
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
    end

    defp enc_read(c, table) do
      enc_domain(table)
      |> Selecto.configure(c, adapter: Adapter, validate: false)
      |> Selecto.with_tenant("café")
      |> Selecto.select(["id", "note"])
      |> Selecto.execute()
    end

    defp enc_write(c, table, note) do
      scope = {:eq, {:field, "org"}, {:literal, "café"}}

      {:ok, command} =
        Command.new(%{
          operation: :update,
          relation: table,
          predicate: scope,
          assignments: [%{field: "note", value: {:literal, note}}],
          expected_cardinality: :many,
          metadata: %{post_image: %{predicate: scope, key: ["id"]}}
        })

      Adapter.execute_write_unsafe(c, command, [])
    end

    defp enc_stream(c, table) do
      {:ok, stream} =
        Adapter.stream(c, "SELECT id, note FROM public.#{table} WHERE org = $1", ["café"],
          selecto_read: true
        )

      Enum.to_list(stream)
    end

    defp notes(c, table), do: sql!(c, "SELECT note FROM public.#{table} ORDER BY id")

    test "UTF8: a tenant reads, streams and writes its own row", ctx do
      c = ctx.connection
      assert {:ok, {[[1, "mine"]], _, _}} = enc_read(c, ctx.enc)
      assert [{[1, "mine"], _}] = enc_stream(c, ctx.enc)
      assert {:ok, %{affected_rows: 1}} = enc_write(c, ctx.enc, "w")
      assert notes(c, ctx.enc) == [["w"], ["SECRET"]]
    end

    test "set on the session: reads, streams and writes refuse", ctx do
      c = ctx.connection
      sql!(c, "SELECT pg_catalog.set_config('client_encoding', 'LATIN1', false)")

      assert {:error, %{details: %{reason: :client_encoding}}} = enc_read(c, ctx.enc)
      assert_raise RuntimeError, ~r/client_encoding/, fn -> enc_stream(c, ctx.enc) end
      assert {:error, %{details: %{reason: :client_encoding}}} = enc_write(c, ctx.enc, "w")

      sql!(c, "SELECT pg_catalog.set_config('client_encoding', 'UTF8', false)")
      assert notes(c, ctx.enc) == [["mine"], ["SECRET"]]
    end

    test "a connection default refuses too", ctx do
      opts =
        ConnectionOptions.options()
        |> Keyword.put(:pool_size, 1)
        |> Keyword.put(:parameters, client_encoding: "LATIN1")

      {:ok, c} = Adapter.connect(opts)
      Process.unlink(c)

      assert {:error, %{details: %{reason: :client_encoding}}} = enc_read(c, ctx.enc)
      assert_raise RuntimeError, ~r/client_encoding/, fn -> enc_stream(c, ctx.enc) end
      assert {:error, %{details: %{reason: :client_encoding}}} = enc_write(c, ctx.enc, "w")
      GenServer.stop(c)
    end

    test "a plant setting it during a write refuses, and the write rolls back", ctx do
      c = ctx.connection
      assert {:error, %{details: %{reason: :client_encoding}}} = enc_write(c, ctx.enc, "plant")

      # The rollback took the setting with it.
      assert sql!(c, "SELECT pg_catalog.current_setting('client_encoding')") == [["UTF8"]]
      assert notes(c, ctx.enc) == [["mine"], ["SECRET"]]
    end
  end

  test "a connection error while preparing a read stays an error, not a raise", ctx do
    pid = spawn(fn -> :ok end)
    Process.sleep(10)

    domain = %{
      name: "H",
      source: %{
        source_table: "h",
        primary_key: :id,
        fields: [:id],
        redact_fields: [],
        columns: %{id: %{type: :integer}},
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }

    selecto = Selecto.configure(domain, ctx.connection, adapter: Adapter, validate: false)
    assert {:error, _} = Adapter.prepare_read(selecto, {:pool, %{pool: pid}})
  end

  test "a string literal cast to integer converts on the server, as natively", ctx do
    table = ctx.table

    cast = fn value ->
      columns = %{
        id: %{type: :integer},
        org: %{type: :string},
        x: %{
          type: :integer,
          computed: %{kind: :expression, expression: ["cast", ["literal", value], "integer"]}
        }
      }

      %{
        name: "H",
        source: %{
          source_table: table,
          primary_key: :id,
          fields: [:id, :org, :x],
          redact_fields: [],
          columns: columns,
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      |> Selecto.configure(ctx.connection, adapter: Adapter, validate: false)
      |> Selecto.select(["id", "x"])
      |> Selecto.order_by(["id"])
      |> Selecto.limit(1)
      |> Selecto.execute()
    end

    assert {:ok, {[[1, 1]], _, _}} = cast.("1")

    for value <- ["1.5", "-1.5", "2.5", "abc", "99999999999999999999999"] do
      assert {:error, _} = cast.(value), value
    end
  end

  test "an explicit cast of a cast keeps both, as natively", ctx do
    run = fn expression, type ->
      columns = %{
        id: %{type: :integer},
        org: %{type: :string},
        x: %{type: type, computed: %{kind: :expression, expression: expression}}
      }

      %{
        name: "H",
        source: %{
          source_table: ctx.table,
          primary_key: :id,
          fields: [:id, :org, :x],
          redact_fields: [],
          columns: columns,
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }
      |> Selecto.configure(ctx.connection, adapter: Adapter, validate: false)
      |> Selecto.select(["id", "x"])
      |> Selecto.order_by(["id"])
      |> Selecto.limit(1)
      |> Selecto.execute()
    end

    assert {:ok, {[[1, 2]], _, _}} =
             run.(["cast", ["cast", ["literal", "1.5"], "decimal"], "integer"], :integer)

    assert {:ok, {[[1, "7"]], _, _}} =
             run.(["cast", ["cast", ["literal", "007"], "integer"], "string"], :string)

    assert {:ok, {[[1, "7"]], _, _}} =
             run.(["cast", ["literal", "007", "integer"], "string"], :string)
  end
end
