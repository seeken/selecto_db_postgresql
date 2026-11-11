defmodule SelectoDBPostgreSQL.ExtensionOperatorIntegrationTest do
  # Extension types (hstore, ltree, postgis) in their own databases: catalog
  # facts, derived outputs, deferrable triggers and read-only reads.
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  defp options, do: SelectoDBPostgreSQL.Verification.ConnectionOptions.options()

  defp exec!(c, sql) do
    case Adapter.execute(c, sql, [], []) do
      {:ok, result} -> result
      other -> flunk("#{sql}: #{inspect(other)}")
    end
  end

  defp in_database(setup, fun) do
    prefix = System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")
    database = "#{prefix}_gate_#{System.unique_integer([:positive])}"
    {:ok, admin} = Adapter.connect(Keyword.put(options(), :pool_size, 1))
    exec!(admin, ~s|CREATE DATABASE "#{database}"|)
    opts = options() |> Keyword.put(:pool_size, 1) |> Keyword.put(:database, database)

    try do
      {:ok, a} = Adapter.connect(opts)
      for sql <- setup, do: exec!(a, sql)
      {:ok, c} = Adapter.connect(opts)
      fun.(a, c)
      GenServer.stop(c)
      GenServer.stop(a)
    after
      exec!(admin, ~s|DROP DATABASE IF EXISTS "#{database}" WITH (FORCE)|)
      GenServer.stop(admin)
    end
  end

  defp guarded(c, sql, params \\ []) do
    # What Selecto generates.
    case Adapter.execute(c, sql, params,
           selecto_read: true,
           selecto_generated: true
         ) do
      {:ok, %{rows: rows}} -> rows
      {:error, %{details: %{reason: reason}}} -> {:refused, reason}
      other -> other
    end
  end

  test "catalog facts key columns exactly (R56)" do
    in_database(
      [
        "CREATE EXTENSION citext",
        ~s|CREATE TABLE cv (id int PRIMARY KEY, org text NOT NULL, "ORG" citext, note text)|,
        ~s|INSERT INTO cv VALUES (1, 't1', 'x', 'n'), (2, 'T1', 'x', 'n'), (3, 't2', 'x', 'n')|,
        ~s|CREATE TABLE cm (id int PRIMARY KEY, org citext, "ORG" text NOT NULL, note text)|,
        ~s|INSERT INTO cm VALUES (1, 'x', 't1', 'n'), (2, 'x', 'T1', 'n'), (3, 'x', 't2', 'n')|
      ],
      fn _a, c ->
        for {sql, rows} <- [
              {"SELECT id FROM public.cv WHERE org = 't1'", [[1]]},
              {"SELECT id FROM public.cv WHERE cv.org = 't1'", [[1]]},
              {~s|SELECT id FROM public.cv WHERE "ORG" = 'X' ORDER BY id|, [[1], [2], [3]]},
              {~s|SELECT id FROM public.cm WHERE "ORG" = 't1'|, [[1]]},
              {"SELECT id FROM public.cm WHERE org = 'X' ORDER BY id", [[1], [2], [3]]},
              {~s|SELECT id FROM public.cm c WHERE c."ORG" = 't1'|, [[1]]},
              {~s|SELECT s.id FROM (SELECT id, "ORG" AS k, org AS j FROM public.cm) s WHERE s.k = 't1'|,
               [[1]]},
              {~s|WITH m AS (SELECT id, "ORG" FROM public.cm) SELECT id FROM m WHERE "ORG" = 't1'|,
               [[1]]},
              {~s|WITH m AS (SELECT id, org, "ORG" FROM public.cm) SELECT id FROM m WHERE org = 'X' ORDER BY id|,
               [[1], [2], [3]]}
            ] do
          assert guarded(c, sql) == rows, sql
        end
      end
    )
  end

  test "a derived output of a literal or parameter is text, not untyped (R57)" do
    in_database(
      [
        "CREATE EXTENSION citext",
        "CREATE TABLE t (id int PRIMARY KEY, c citext, vc varchar(10))",
        "INSERT INTO t VALUES (1, 'Abc', 'Abc'), (2, 'ABC', 'ABC'), (3, 'xyz', 'xyz')"
      ],
      fn _a, c ->
        for sql <- [
              "WITH lc AS (SELECT 'ABC' AS v) SELECT t.id FROM public.t, lc WHERE t.c = lc.v",
              "WITH lc AS (SELECT 'ABC' AS v) SELECT t.id FROM public.t JOIN lc ON lc.v = t.c",
              "WITH lc(v) AS (VALUES ('ABC')) SELECT t.id FROM public.t, lc WHERE t.c = lc.v",
              "WITH RECURSIVE lc(v, n) AS (SELECT 'ABC', 1 UNION ALL SELECT v, n + 1 FROM lc WHERE n < 1) " <>
                "SELECT DISTINCT t.id FROM public.t, lc WHERE t.c = lc.v",
              "WITH l1 AS (SELECT 'ABC' AS v), l2 AS (SELECT v AS w FROM l1) " <>
                "SELECT t.id FROM public.t, l2 WHERE t.c = l2.w",
              "SELECT t.id FROM public.t, (SELECT 'ABC' AS v) d WHERE t.c = d.v",
              "SELECT t.id FROM public.t, LATERAL (SELECT 'ABC' AS v) d WHERE t.c = d.v",
              "SELECT t.id FROM public.t WHERE t.c = (SELECT 'ABC')",
              "SELECT t.id FROM public.t WHERE t.c IN (SELECT 'ABC')"
            ] do
          assert guarded(c, sql) == [[2]], sql
        end

        # Only the bare literal or parameter is untyped: citext's operator.
        assert guarded(c, "SELECT id FROM public.t WHERE c = 'ABC' ORDER BY id") == [[1], [2]]

        assert guarded(c, "SELECT id FROM public.t WHERE c = $1 ORDER BY id", ["ABC"]) == [
                 [1],
                 [2]
               ]

        assert guarded(c, "SELECT id FROM public.t WHERE c = vc ORDER BY id") ==
                 [[1], [2], [3]]
      end
    )
  end

  test "another session's deferrable trigger never refuses a read or a stream" do
    trigger =
      "CREATE CONSTRAINT TRIGGER r61 AFTER UPDATE ON other DEFERRABLE INITIALLY DEFERRED " <>
        "FOR EACH ROW EXECUTE FUNCTION tg()"

    in_database(
      [
        "CREATE TABLE r (id int PRIMARY KEY, note text)",
        "INSERT INTO r SELECT g, 'n' FROM generate_series(1, 5) g",
        "CREATE TABLE other (id int PRIMARY KEY)",
        "CREATE FUNCTION tg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$"
      ],
      fn a, c ->
        # A slow read in session A; session B commits the trigger meanwhile.
        slow =
          Task.async(fn ->
            Adapter.execute(c, "SELECT count(*), max(pg_sleep(0.6)::text) FROM public.r", [],
              selecto_read: true,
              timeout: 5_000
            )
          end)

        Process.sleep(200)
        exec!(a, trigger)
        assert {:ok, %{rows: [[5, _]]}} = Task.await(slow, 6_000)
        exec!(a, "DROP TRIGGER r61 ON other")

        # A stream, batch by batch, across the same commit.
        fields = ~w(id note)a

        domain = %{
          name: "r",
          source: %{
            source_table: "r",
            primary_key: :id,
            fields: fields,
            redact_fields: [],
            columns: %{id: %{type: :integer}, note: %{type: :string}},
            associations: %{}
          },
          schemas: %{},
          joins: %{}
        }

        {:ok, stream} =
          domain
          |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
          |> Selecto.select(["id"])
          |> Selecto.order_by(["id"])
          |> Selecto.execute_stream(max_rows: 1, receive_timeout: 5_000)

        seen =
          Enum.reduce(stream, 0, fn _row, count ->
            if count == 1, do: exec!(a, trigger)
            count + 1
          end)

        assert seen == 5

        # With the trigger committed, reads stand.
        assert {:ok, %{rows: [[5]]}} =
                 Adapter.execute(c, "SELECT count(*) FROM public.r", [], selecto_read: true)
      end
    )
  end

  test "a read cannot write: a function it reaches meets a read-only transaction" do
    {:ok, c} = Adapter.connect(Keyword.put(options(), :pool_size, 1))
    schema = "selecto_ro_#{System.unique_integer([:positive])}"

    for sql <- [
          "CREATE SCHEMA #{schema}",
          "CREATE TABLE #{schema}.ro (id int PRIMARY KEY, note text)",
          "INSERT INTO #{schema}.ro VALUES (1, 'n')",
          "CREATE FUNCTION #{schema}.w() RETURNS int LANGUAGE plpgsql AS " <>
            "$$ BEGIN UPDATE #{schema}.ro SET note = 'written'; RETURN 1; END $$"
        ],
        do: exec!(c, sql)

    try do
      read = fn conn ->
        Adapter.execute(conn, "SELECT #{schema}.w() FROM #{schema}.ro", [],
          selecto_read: true,
          selecto_declared_functions: ["#{schema}.w"]
        )
      end

      assert {:error, %{details: %{reason: :read_only_read}}} = read.(c)
      assert %{rows: [["n"]]} = exec!(c, "SELECT note FROM #{schema}.ro")

      # What merely says "for share" takes no lock.
      for sql <- [
            ~s|SELECT #{schema}.w() AS "for share" FROM #{schema}.ro|,
            "SELECT #{schema}.w() /* for update */ FROM #{schema}.ro",
            "SELECT #{schema}.w() FROM #{schema}.ro WHERE 'x FOR UPDATE' OPERATOR(pg_catalog.<>) ''"
          ] do
        assert {:error, %{details: %{reason: :read_only_read}}} =
                 Adapter.execute(c, sql, [],
                   selecto_read: true,
                   selecto_declared_functions: ["#{schema}.w"]
                 )
      end

      # In a host transaction the read is the host's own statement: what the
      # function writes belongs to that transaction, which stays writable.
      Postgrex.transaction(c, fn conn ->
        assert {:ok, %{rows: [[1]]}} = read.(conn)

        assert {:ok, %{num_rows: 1}} =
                 Postgrex.query(conn, "UPDATE #{schema}.ro SET note = 'host'", [])

        assert {:ok, %{rows: [[1]]}} =
                 Adapter.execute(conn, "SELECT id FROM #{schema}.ro", [], selecto_read: true)

        assert {:ok, %{num_rows: 1}} =
                 Postgrex.query(conn, "UPDATE #{schema}.ro SET note = 'after'", [])
      end)

      assert %{rows: [["after"]]} = exec!(c, "SELECT note FROM #{schema}.ro")

      # A generated row lock cannot be taken read-only: such a read stays writable.
      assert {:ok, %{rows: [[1]]}} =
               Adapter.execute(c, "SELECT id FROM #{schema}.ro FOR SHARE", [],
                 selecto_read: true,
                 selecto_generated: true
               )

      # Hand-built text always is read-only: only that stops it writing.
      assert {:error, %{details: %{reason: :read_only_read}}} =
               Adapter.execute(c, "SELECT id FROM #{schema}.ro FOR SHARE", [], selecto_read: true)
    after
      exec!(c, "DROP SCHEMA #{schema} CASCADE")
      GenServer.stop(c)
    end
  end
end
