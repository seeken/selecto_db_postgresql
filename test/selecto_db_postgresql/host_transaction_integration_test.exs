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

  test "a write joins the host's transaction, and its ROLLBACK takes it", ctx do
    c = ctx.connection
    [[backend]] = sql!(c, "SELECT pg_backend_pid()")
    sql!(c, "BEGIN")
    assert {:ok, %{affected_rows: 1}} = write(c, ctx.table, 1)
    sql!(c, "INSERT INTO public.#{ctx.table} VALUES (9, 't1', 'host')")
    assert sql!(c, "SELECT pg_backend_pid()") == [[backend]]
    sql!(c, "ROLLBACK")

    assert sql!(c, "SELECT count(*) FROM public.#{ctx.table} WHERE id = 9") == [[0]]
    assert sql!(c, "SELECT note FROM public.#{ctx.table} WHERE id = 1") == [["n"]]
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
    for query <- [forged, {:selecto_control, "COMMIT"}] do
      assert_raise ArgumentError, fn -> Adapter.execute(ctx.connection, query, [], []) end
    end

    assert sql!(ctx.connection, "SELECT count(*) FROM public.#{ctx.table}") == [[2]]
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
end
