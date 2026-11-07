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

  describe "a read keeps its relation locks to the host's end (R69)" do
    setup ctx do
      opts = Keyword.put(ConnectionOptions.options(), :pool_size, 1)
      {:ok, other} = Adapter.connect(opts)
      Process.unlink(other)
      view = "selecto_host_v_#{System.unique_integer([:positive])}"
      sql!(other, "SET lock_timeout = '150ms'")
      sql!(other, "CREATE VIEW public.#{view} AS SELECT id, note FROM public.#{ctx.table}")

      on_exit(fn ->
        {:ok, d} = Adapter.connect(opts)
        Adapter.execute(d, "DROP VIEW IF EXISTS public.#{view}", [], [])
        GenServer.stop(d)
      end)

      %{other: other, view: view}
    end

    defp ddl_blocked?(other, table) do
      statement =
        "ALTER TABLE public.#{table} ADD COLUMN r69_#{System.unique_integer([:positive])} int"

      match?(
        {:error, %Postgrex.Error{postgres: %{code: :lock_not_available}}},
        Adapter.execute(other, statement, [], [])
      )
    end

    defp generated(conn, relation) do
      Adapter.execute(conn, "SELECT id FROM #{relation}", [],
        selecto_read: true,
        selecto_generated: true,
        selecto_qualified_relations: %{
          SelectoDBPostgreSQL.RelationPath.path_key() => ["public"],
          relation => ~s("public"."#{relation}")
        }
      )
    end

    test "generated reads, of a table and through a view", ctx do
      for relation <- [ctx.table, ctx.view] do
        {:ok, blocked?} =
          Postgrex.transaction(ctx.connection, fn conn ->
            assert {:ok, %{rows: [_, _]}} = generated(conn, relation)
            ddl_blocked?(ctx.other, ctx.table)
          end)

        assert blocked?, relation
      end
    end

    test "hand-built reads, of a table and through a view", ctx do
      for relation <- [ctx.table, ctx.view] do
        {:ok, blocked?} =
          Postgrex.transaction(ctx.connection, fn conn ->
            assert {:ok, %{rows: [_, _]}} =
                     Adapter.execute(conn, "SELECT id FROM public.#{relation}", [],
                       selecto_read: true
                     )

            ddl_blocked?(ctx.other, ctx.table)
          end)

        assert blocked?, relation
      end
    end

    test "the plan decides, not the words", ctx do
      words = "selecto_host_words_#{System.unique_integer([:positive])}"

      sql!(
        ctx.other,
        ~s|CREATE TABLE public.#{words} (id int PRIMARY KEY, update int, "delete" int)|
      )

      try do
        for sql <- [
              "SELECT update FROM public.#{words}",
              ~s|SELECT "delete" FROM public.#{words}|,
              "SELECT id AS merge FROM public.#{words}",
              "SELECT id FROM public.#{words} WHERE 'x\nModifyTable' <> ''"
            ] do
          {:ok, blocked?} =
            Postgrex.transaction(ctx.connection, fn conn ->
              assert {:ok, _} = Adapter.execute(conn, sql, [], selecto_read: true)
              ddl_blocked?(ctx.other, words)
            end)

          assert blocked?, sql
        end
      after
        sql!(ctx.other, "DROP TABLE public.#{words}")
      end
    end

    test "text that writes is not planned for a write lock, and refuses", ctx do
      {:ok, locks} =
        Postgrex.transaction(ctx.connection, fn conn ->
          assert {:error, %{details: %{reason: :read_only_read}}} =
                   Adapter.execute(
                     conn,
                     "WITH d AS (DELETE FROM public.#{ctx.table} RETURNING id) SELECT * FROM d",
                     [],
                     selecto_read: true
                   )

          Postgrex.query!(
            conn,
            "SELECT mode FROM pg_locks WHERE pid = pg_backend_pid() AND relation = 'public.#{ctx.table}'::regclass",
            []
          ).rows
        end)

      assert locks == []
    end

    test "a relation that cannot be read leaves the host's transaction alive", ctx do
      {:ok, rows} =
        Postgrex.transaction(ctx.connection, fn conn ->
          assert {:error, %Postgrex.Error{postgres: %{code: :undefined_table}}} =
                   Adapter.execute(conn, "SELECT 1 FROM public.selecto_no_such", [],
                     selecto_read: true
                   )

          Postgrex.query!(conn, "SELECT 1", []).rows
        end)

      assert rows == [[1]]
    end

    test "a lock timeout taking them stands, and the host's transaction lives", ctx do
      sql!(ctx.other, "BEGIN")
      sql!(ctx.other, "LOCK TABLE public.#{ctx.table} IN ACCESS EXCLUSIVE MODE")

      {:ok, rows} =
        Postgrex.transaction(ctx.connection, fn conn ->
          Postgrex.query!(conn, "SET LOCAL lock_timeout = '100ms'", [])

          assert {:error, %Postgrex.Error{postgres: %{code: :lock_not_available}}} =
                   generated(conn, ctx.table)

          Postgrex.query!(conn, "SELECT 1", []).rows
        end)

      sql!(ctx.other, "ROLLBACK")
      assert rows == [[1]]
    end
  end

  describe "a read with a deadline on a connection the host holds (H-A)" do
    setup ctx do
      slow = "selecto_host_slow_#{System.unique_integer([:positive])}"

      sql!(
        ctx.connection,
        "CREATE VIEW public.#{slow} AS SELECT id, pg_catalog.pg_sleep(0.2)::text AS z FROM public.#{ctx.table}"
      )

      on_exit(fn ->
        {:ok, d} = Adapter.connect(Keyword.put(ConnectionOptions.options(), :pool_size, 1))
        Adapter.execute(d, "DROP VIEW IF EXISTS public.#{slow}", [], [])
        GenServer.stop(d)
      end)

      %{slow: slow}
    end

    defp timed_out(conn, slow) do
      assert {:error, %Postgrex.Error{postgres: %{code: :query_canceled}}} =
               Adapter.execute(conn, "SELECT z FROM public.#{slow}", [],
                 selecto_read: true,
                 timeout: 60
               )
    end

    defp session(conn) do
      Postgrex.query!(
        conn,
        "SELECT current_setting('transaction_read_only'), current_setting('search_path'), " <>
          "current_setting('statement_timeout')",
        []
      ).rows
    end

    test "Postgrex.transaction: the host's work and settings survive", ctx do
      c = ctx.connection
      native = session(c)

      assert {:ok, :done} =
               Postgrex.transaction(c, fn t ->
                 Postgrex.query!(t, "INSERT INTO public.#{ctx.table} VALUES (7, 't1', 'a')", [])
                 timed_out(t, ctx.slow)
                 assert session(t) == native
                 Postgrex.query!(t, "INSERT INTO public.#{ctx.table} VALUES (8, 't1', 'b')", [])
                 :done
               end)

      assert sql!(c, "SELECT count(*) FROM public.#{ctx.table} WHERE id IN (7, 8)") == [[2]]
    end

    test "Selecto.transaction: the same", ctx do
      c = ctx.connection
      native = session(c)

      domain = %{
        name: "D",
        source: %{
          source_table: ctx.slow,
          primary_key: :id,
          fields: [:id, :z],
          redact_fields: [],
          columns: %{id: %{type: :integer}, z: %{type: :string}},
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }

      selecto = Selecto.configure(domain, c, adapter: Adapter, validate: false)

      assert {:ok, _} =
               Selecto.transaction(selecto, fn tx ->
                 conn = Selecto.Runtime.Context.connection(tx)

                 Postgrex.query!(
                   conn,
                   "INSERT INTO public.#{ctx.table} VALUES (7, 't1', 'a')",
                   []
                 )

                 assert {:error, %Selecto.Error{type: :timeout_error}} =
                          tx |> Selecto.select(["z"]) |> Selecto.execute(timeout: 60)

                 assert session(conn) == native
                 {:ok, :done}
               end)

      assert sql!(c, "SELECT count(*) FROM public.#{ctx.table} WHERE id = 7") == [[1]]
    end

    test "an idle DBConnection.run handle stays idle and the pool stays clean", ctx do
      c = ctx.connection

      DBConnection.run(c, fn k ->
        timed_out(k, ctx.slow)
        assert DBConnection.status(k) == :idle
        Postgrex.query!(k, "INSERT INTO public.#{ctx.table} VALUES (7, 't1', 'a')", [])
      end)

      assert sql!(c, "SELECT count(*) FROM public.#{ctx.table} WHERE id = 7") == [[1]]
      assert DBConnection.run(c, &DBConnection.status/1) == :idle
    end

    test "a shorter statement_timeout of the host's stands", ctx do
      c = ctx.connection

      assert {:ok, _} =
               Postgrex.transaction(c, fn t ->
                 Postgrex.query!(t, "SET LOCAL statement_timeout = 40", [])

                 for opts <- [[], [selecto_generated: true]] do
                   {micros, result} =
                     :timer.tc(fn ->
                       Adapter.execute(
                         t,
                         "SELECT z FROM public.#{ctx.slow}",
                         [],
                         [
                           selecto_read: true,
                           timeout: 5_000,
                           selecto_qualified_relations: %{
                             SelectoDBPostgreSQL.RelationPath.path_key() => ["public"]
                           }
                         ] ++ opts
                       )
                     end)

                   assert {:error, %Postgrex.Error{postgres: %{code: :query_canceled}}} = result
                   assert micros < 2_000_000
                 end

                 assert [["40ms"]] =
                          Postgrex.query!(t, "SELECT current_setting('statement_timeout')", []).rows
               end)
    end

    test "a text BEGIN on a pid survives it too", ctx do
      c = ctx.connection
      [[backend]] = sql!(c, "SELECT pg_backend_pid()")
      sql!(c, "BEGIN")
      sql!(c, "INSERT INTO public.#{ctx.table} VALUES (7, 't1', 'a')")
      timed_out(c, ctx.slow)
      assert sql!(c, "SELECT pg_backend_pid()") == [[backend]]
      sql!(c, "COMMIT")
      assert sql!(c, "SELECT count(*) FROM public.#{ctx.table} WHERE id = 7") == [[1]]
    end

    test "the deadline covers planning: a lock wait ends, and the host lives", ctx do
      {:ok, other} = Adapter.connect(Keyword.put(ConnectionOptions.options(), :pool_size, 1))
      Process.unlink(other)
      sql!(other, "BEGIN")
      sql!(other, "LOCK TABLE public.#{ctx.table} IN ACCESS EXCLUSIVE MODE")

      assert {:ok, rows} =
               Postgrex.transaction(ctx.connection, fn t ->
                 assert {:error, %Postgrex.Error{postgres: %{code: :query_canceled}}} =
                          Adapter.execute(t, "SELECT id FROM public.#{ctx.table}", [],
                            selecto_read: true,
                            timeout: 80
                          )

                 assert [["0"]] =
                          Postgrex.query!(t, "SELECT current_setting('statement_timeout')", []).rows

                 Postgrex.query!(t, "SELECT 1", []).rows
               end)

      sql!(other, "ROLLBACK")
      GenServer.stop(other)
      assert rows == [[1]]
    end
  end

  describe "hand-built reads run in the host's session (R71)" do
    setup ctx do
      schema = "selecto_host_app_#{System.unique_integer([:positive])}"

      for sql <- [
            "CREATE SCHEMA #{schema}",
            "CREATE FUNCTION #{schema}.who() RETURNS text LANGUAGE sql AS $$SELECT 'app'$$",
            "CREATE FUNCTION public.who() RETURNS text LANGUAGE sql AS $$SELECT 'public'$$",
            "CREATE TABLE #{schema}.#{ctx.table} (id int)",
            "INSERT INTO #{schema}.#{ctx.table} VALUES (99)"
          ],
          do: sql!(ctx.connection, sql)

      on_exit(fn ->
        {:ok, d} = Adapter.connect(Keyword.put(ConnectionOptions.options(), :pool_size, 1))
        Adapter.execute(d, "DROP SCHEMA #{schema} CASCADE", [], [])
        Adapter.execute(d, "DROP FUNCTION IF EXISTS public.who()", [], [])
        GenServer.stop(d)
      end)

      %{schema: schema}
    end

    defp same_as_driver(conn, sql) do
      assert {:ok, %{rows: native}} = Adapter.execute(conn, sql, [], [])
      assert {:ok, %{rows: ^native}} = Adapter.execute(conn, sql, [], selecto_read: true), sql
      native
    end

    test "under a host search_path, functions, relations and settings are the driver's", ctx do
      c = ctx.connection
      sql!(c, "SET search_path = #{ctx.schema}, public")

      assert same_as_driver(c, "SELECT who()") == [["app"]]
      assert same_as_driver(c, "SELECT id FROM #{ctx.table}") == [[99]]
      assert [[_]] = same_as_driver(c, "SELECT current_schema()")
      assert [[_]] = same_as_driver(c, "SHOW search_path")
      sql!(c, "SET search_path TO DEFAULT")
    end

    test "SET LOCAL in a host transaction, and DateStyle", ctx do
      c = ctx.connection

      assert {:ok, _} =
               Postgrex.transaction(c, fn t ->
                 Postgrex.query!(t, "SET LOCAL search_path = #{ctx.schema}, public", [])
                 Postgrex.query!(t, "SET LOCAL DateStyle = 'SQL, DMY'", [])
                 assert same_as_driver(t, "SELECT who()") == [["app"]]
                 assert same_as_driver(t, "SELECT date '2026-11-06'::text") == [["06/11/2026"]]
                 assert [["SQL, DMY"]] = same_as_driver(t, "SHOW DateStyle")
               end)
    end

    test "a temp table is the session's", ctx do
      c = ctx.connection
      sql!(c, "CREATE TEMP TABLE #{ctx.table} (id int)")
      sql!(c, "INSERT INTO pg_temp.#{ctx.table} VALUES (42)")
      assert same_as_driver(c, "SELECT id FROM #{ctx.table}") == [[42]]
      assert same_as_driver(c, "SELECT 1") == [[1]]
      sql!(c, "DROP TABLE pg_temp.#{ctx.table}")
    end

    test "a set operation of generated reads compiles after prepare_read", ctx do
      domain = %{
        name: "D",
        source: %{
          source_table: ctx.table,
          source_schema: "public",
          primary_key: :id,
          fields: [:id],
          redact_fields: [],
          columns: %{id: %{type: :integer}},
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }

      left =
        domain
        |> Selecto.configure(ctx.connection, adapter: Adapter, validate: false)
        |> Selecto.select(["id"])

      assert {:ok, {rows, _, _}} = left |> Selecto.union(left) |> Selecto.execute()
      assert Enum.sort(rows) == [[1], [2]]
    end

    test "a generated read keeps its pin and guards", ctx do
      c = ctx.connection
      sql!(c, "SET search_path = #{ctx.schema}, public")

      domain = %{
        name: "D",
        source: %{
          source_table: ctx.table,
          primary_key: :id,
          fields: [:id],
          redact_fields: [],
          columns: %{id: %{type: :integer}},
          associations: %{}
        },
        schemas: %{},
        joins: %{}
      }

      read = fn filters ->
        domain
        |> Selecto.configure(c, adapter: Adapter, validate: false)
        |> Selecto.select(["id"])
        |> then(&Enum.reduce(filters, &1, fn f, q -> Selecto.filter(q, f) end))
        |> Selecto.execute()
      end

      assert {:error, %{details: %{reason: :relation_ambiguous}}} = read.([])

      # A host fragment does not make the statement the host's (R71).
      assert {:error, %{details: %{reason: :relation_ambiguous}}} =
               read.([{:raw_sql_filter, "selecto_root.id > 0"}])

      sql!(c, "SET search_path TO DEFAULT")
    end

    test "a statement carrying a host fragment keeps its relation, pin and tenant scope", ctx do
      c = ctx.connection

      domain = %{
        name: "D",
        source: %{
          source_table: ctx.table,
          source_schema: "public",
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

      read = fn extra ->
        domain
        |> Selecto.configure(c, adapter: Adapter, validate: false)
        |> Selecto.with_tenant("t1")
        |> Selecto.select(["id", extra])
        |> Selecto.order_by("id")
        |> Selecto.execute()
      end

      sql!(c, "SET search_path = #{ctx.schema}, public")

      # Declared public.table, not the namesake the host's path finds first, and
      # the fragment runs under the pin: public.who(), verbatim.
      assert {:ok, {[[1, "public"], [2, "public"]], _, _}} = read.({:raw_sql, "who() AS w"})
      sql!(c, "SET search_path TO DEFAULT")

      # A host TEMP namesake does not capture it either.
      sql!(c, "CREATE TEMP TABLE #{ctx.table} (id int, org text)")
      sql!(c, "INSERT INTO pg_temp.#{ctx.table} VALUES (42, 't1')")
      assert {:ok, {[[1, 1], [2, 1]], _, _}} = read.({:raw_sql, "1 AS one"})
      sql!(c, "DROP TABLE pg_temp.#{ctx.table}")
    end
  end

  test "a stream refuses inside the host's transaction, which goes on", ctx do
    c = ctx.connection
    [[backend]] = sql!(c, "SELECT pg_backend_pid()")
    sql!(c, "BEGIN")

    assert {:error, %Selecto.Error{details: %{reason: :stream_in_transaction}}} =
             Adapter.stream(c, "SELECT id FROM public.#{ctx.table}", [], selecto_read: true)

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
      with {:ok, stream} <-
             Adapter.stream(c, "SELECT id, note FROM public.#{table} WHERE org = $1", ["café"],
               selecto_read: true
             ) do
        Enum.to_list(stream)
      end
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
      assert {:error, %{details: %{reason: :client_encoding}}} = enc_stream(c, ctx.enc)
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
      assert {:error, %{details: %{reason: :client_encoding}}} = enc_stream(c, ctx.enc)
      assert {:error, %{details: %{reason: :client_encoding}}} = enc_write(c, ctx.enc, "w")
      GenServer.stop(c)
    end

    test "UNICODE is UTF8 by another name", ctx do
      c = ctx.connection
      sql!(c, "SELECT pg_catalog.set_config('client_encoding', 'UNICODE', false)")
      assert {:ok, {[[1, "mine"]], _, _}} = enc_read(c, ctx.enc)
      assert {:ok, %{affected_rows: 1}} = enc_write(c, ctx.enc, "w")
      sql!(c, "SELECT pg_catalog.set_config('client_encoding', 'UTF8', false)")
    end

    test "a plant setting it during a write refuses, and the write rolls back", ctx do
      c = ctx.connection
      assert {:error, %{details: %{reason: :client_encoding}}} = enc_write(c, ctx.enc, "plant")

      # The rollback took the setting with it.
      assert sql!(c, "SELECT pg_catalog.current_setting('client_encoding')") == [["UTF8"]]
      assert notes(c, ctx.enc) == [["mine"], ["SECRET"]]
    end
  end

  describe "CTE names (H1)" do
    setup ctx do
      relation = "selecto_cte_#{System.unique_integer([:positive])}"

      {:ok, _} =
        Adapter.execute(
          ctx.connection,
          "CREATE TABLE public.#{relation} AS SELECT 'REAL'::text AS v",
          [],
          []
        )

      on_exit(fn ->
        {:ok, d} = Adapter.connect(Keyword.put(ConnectionOptions.options(), :pool_size, 1))
        Adapter.execute(d, "DROP TABLE IF EXISTS public.#{relation}", [], [])
        GenServer.stop(d)
      end)

      %{relation: relation}
    end

    test "a CTE is never rewritten to a real table of its name", ctx do
      r = ctx.relation

      for sql <- [
            "WITH a AS (SELECT 1),#{r} AS (SELECT 'CTE' AS v) SELECT v FROM #{r}",
            "WITH a AS (SELECT 1) , /* c */ #{r} AS (SELECT 'CTE' AS v) SELECT v FROM #{r}",
            "WITH a AS (SELECT 1), -- c\n#{r} AS (SELECT 'CTE' AS v) SELECT v FROM #{r}",
            ~s|WITH #{r}("v") AS (SELECT 'CTE') SELECT v FROM #{r}|,
            ~s|WITH #{r} ("a)b", v) AS (SELECT 1, 'CTE') SELECT v FROM #{r}|,
            ~s|WITH U&"#{r}" AS (SELECT 'CTE' AS v) SELECT v FROM #{r}|,
            "WITH #{r} AS NOT MATERIALIZED (SELECT 'CTE' AS v) SELECT v FROM #{r}"
          ] do
        result = Adapter.execute(ctx.connection, sql, [], selecto_read: true)

        assert match?({:ok, %{rows: [["CTE"]]}}, result) or
                 match?({:error, %{details: _}}, result),
               sql <> inspect(result)
      end
    end

    test "SEARCH and CYCLE do not end the list", ctx do
      r = ctx.relation
      body = "a(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM a WHERE n < 2)"

      search =
        "WITH RECURSIVE #{body} SEARCH DEPTH FIRST BY n SET o, #{r} AS (SELECT 'CTE' AS v) " <>
          "SELECT v FROM #{r}"

      assert {:ok, %{rows: [["CTE"]]}} =
               Adapter.execute(ctx.connection, search, [], selecto_read: true)

      # Hand-built text is the server's to read (R68): its CTE, never the table.
      cycle =
        "WITH RECURSIVE #{body} CYCLE n SET is_c USING p, #{r} AS (SELECT 'CTE' AS v) " <>
          "SELECT v FROM #{r}"

      assert {:ok, %{rows: [["CTE"]]}} =
               Adapter.execute(ctx.connection, cycle, [], selecto_read: true)
    end

    test "a WITH list that cannot be read refuses, with no extension type", ctx do
      r = ctx.relation
      sql = "WITH a AS (SELECT 1) UNKNOWN, #{r} AS (SELECT 'CTE' AS v) SELECT v FROM #{r}"
      assert {:error, _} = Adapter.execute(ctx.connection, sql, [], selecto_read: true)
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
