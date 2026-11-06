defmodule SelectoDBPostgreSQL.ExtensionOperatorIntegrationTest do
  # H1 (w3exy): an extension's schema is commonly writable, so an operator
  # planted there resolves as readily as a member. Only an operator that is a
  # pg_depend member of the operand type's extension, for those operand
  # types, is ever named; anything else is pg_catalog's or refuses, typed.
  # The plants here write to another tenant's row when they run. Extensions
  # in their own schema and in public.
  use ExUnit.Case, async: false

  alias Selecto.Write.Command
  alias SelectoDBPostgreSQL.Adapter

  @moduletag :postgres

  defp options, do: SelectoDBPostgreSQL.Verification.ConnectionOptions.options()

  defp exec!(c, sql) do
    case Adapter.execute(c, sql, [], []) do
      {:ok, result} -> result
      other -> flunk("#{sql}: #{inspect(other)}")
    end
  end

  defp plants(ext) do
    pwn = "BEGIN UPDATE public.ht SET note = 'PWNED' WHERE org = 't2'; RETURN true; END"

    fun = fn name, type ->
      "CREATE FUNCTION #{ext}.#{name}(#{type}, #{type}) RETURNS boolean LANGUAGE plpgsql AS $$#{pwn}$$"
    end

    [
      fun.("pwn_h", "#{ext}.hstore"),
      "CREATE OPERATOR #{ext}.< (LEFTARG = #{ext}.hstore, RIGHTARG = #{ext}.hstore, FUNCTION = #{ext}.pwn_h)",
      "CREATE OPERATOR #{ext}.~~ (LEFTARG = #{ext}.hstore, RIGHTARG = #{ext}.hstore, FUNCTION = #{ext}.pwn_h)",
      fun.("pwn_l", "#{ext}.ltree"),
      "CREATE OPERATOR #{ext}.~~ (LEFTARG = #{ext}.ltree, RIGHTARG = #{ext}.ltree, FUNCTION = #{ext}.pwn_l)",
      fun.("pwn_q", "#{ext}.lquery"),
      "CREATE OPERATOR #{ext}.= (LEFTARG = #{ext}.lquery, RIGHTARG = #{ext}.lquery, FUNCTION = #{ext}.pwn_q)"
    ]
  end

  defp domain do
    fields = ~w(id org h l q note)a

    %{
      name: "ht",
      source: %{
        source_table: "ht",
        primary_key: :id,
        tenant_field: :org,
        fields: fields,
        redact_fields: [],
        columns: Map.new(fields, &{&1, %{type: if(&1 == :id, do: :integer, else: :string)}}),
        associations: %{}
      },
      schemas: %{},
      joins: %{}
    }
  end

  for {label, ext, path} <- [
        {"their own schema", "ext", "public, ext"},
        {"public", "public", "public"}
      ] do
    test "a planted operator in #{label} never runs" do
      ext = unquote(ext)
      prefix = System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")
      database = "#{prefix}_extop_#{System.unique_integer([:positive])}"
      {:ok, admin} = Adapter.connect(Keyword.put(options(), :pool_size, 1))
      exec!(admin, ~s|CREATE DATABASE "#{database}"|)
      exec!(admin, ~s|ALTER DATABASE "#{database}" SET search_path = #{unquote(path)}|)
      opts = options() |> Keyword.put(:pool_size, 1) |> Keyword.put(:database, database)

      try do
        {:ok, setup} = Adapter.connect(opts)

        for sql <-
              [
                "CREATE SCHEMA IF NOT EXISTS ext",
                "CREATE EXTENSION hstore SCHEMA #{ext}",
                "CREATE EXTENSION ltree SCHEMA #{ext}",
                "CREATE TABLE ht (id int PRIMARY KEY, org text NOT NULL, h #{ext}.hstore, " <>
                  "l #{ext}.ltree, q #{ext}.lquery, note text)",
                "INSERT INTO ht VALUES (1, 't1', 'a=>1', 'top.a', 'top.*', 'n'), " <>
                  "(2, 't2', 'a=>2', 'top.b', 'top.*', 'n')"
              ] ++ plants(ext),
            do: exec!(setup, sql)

        {:ok, c} = Adapter.connect(opts)

        untouched = fn ->
          assert %{rows: [["n"]]} = exec!(setup, "SELECT note FROM ht WHERE id = 2")
        end

        read = fn filter ->
          domain()
          |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
          |> Selecto.with_tenant("t1")
          |> Selecto.select(["id"])
          |> Selecto.filter(filter)
          |> Selecto.execute()
        end

        for filter <- [
              {"h", {"<", %{"a" => "9"}}},
              {"h", {:like, %{"a" => "9"}}},
              {"l", {:like, "top"}},
              {"l", {:not_like, "top"}},
              {"q", "top.*"},
              {"q", ["top.*"]}
            ] do
          assert {:error, %{details: %{code: :query_rule_not_evaluable}}} = read.(filter),
                 inspect(filter)

          untouched.()
        end

        # Members keep working, operands cast to their types.
        assert {:ok, {[[1]], _, _}} = read.({"l", "top.a"})
        assert {:ok, {[[1]], _, _}} = read.({"l", ["top.a", "x"]})

        hand_built = &Adapter.execute(c, &1, [], selecto_read: true)

        assert {:ok, %{rows: [[1], [2]]}} =
                 hand_built.("SELECT id FROM ht WHERE l ~ 'top.*' ORDER BY id")

        assert {:ok, %{rows: [["1"]]}} = hand_built.("SELECT h -> 'a' FROM ht WHERE id = 1")

        assert {:ok, %{rows: [[1], [2]]}} =
                 hand_built.("SELECT id FROM ht WHERE h ? 'a' ORDER BY id")

        assert {:ok, %{rows: [[1]]}} =
                 hand_built.("SELECT id FROM ht WHERE l @> 'top.a' AND id = 1")

        for sql <- [
              "SELECT id FROM ht WHERE h < 'a=>9'",
              "SELECT id FROM ht WHERE h LIKE 'a=>9'",
              "SELECT id FROM ht WHERE q = 'top.*'"
            ] do
          assert {:error, %{details: %{reason: :operator_not_member}}} = hand_built.(sql), sql
          untouched.()
        end

        # An explicit operator is verified, not trusted.
        assert {:error, %{details: %{reason: :operator_not_member}}} =
                 hand_built.("SELECT id FROM ht WHERE h OPERATOR(#{ext}.<) 'a=>9'")

        assert {:error, %{details: %{reason: :operator_not_member}}} =
                 hand_built.(
                   "SELECT id FROM ht WHERE (h OPERATOR(#{ext}.~~) CAST('a=>9' AS #{ext}.hstore))"
                 )

        untouched.()

        # A write's predicate and its post-image scope.
        scope = {:lt, {:field, "h"}, {:literal, %{"a" => "9"}}}

        {:ok, command} =
          Command.new(%{
            operation: :update,
            relation: "ht",
            predicate: {:and, [{:eq, {:field, "id"}, {:literal, 1}}, scope]},
            assignments: [%{field: "note", value: {:literal, "x"}}],
            expected_cardinality: :many,
            metadata: %{post_image: %{predicate: scope, key: ["id"]}}
          })

        assert {:error, %{details: %{reason: :operator_not_member}}} =
                 Adapter.execute_write_unsafe(c, command, [])

        untouched.()
        assert %{rows: [["n"]]} = exec!(setup, "SELECT note FROM ht WHERE id = 1")

        GenServer.stop(c)
        GenServer.stop(setup)
      after
        exec!(admin, ~s|DROP DATABASE IF EXISTS "#{database}" WITH (FORCE)|)
        GenServer.stop(admin)
      end
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
    case Adapter.execute(c, sql, params, selecto_read: true) do
      {:ok, %{rows: rows}} -> rows
      {:error, %{details: %{reason: reason}}} -> {:refused, reason}
      other -> other
    end
  end

  test "a plant beside an extension's members is ambiguous, never the choice (R58, R60)" do
    in_database(
      [
        "CREATE EXTENSION hstore",
        "CREATE EXTENSION citext",
        "CREATE EXTENSION pgcrypto",
        "CREATE EXTENSION pg_trgm",
        "CREATE TABLE ht (id int PRIMARY KEY, h hstore, c citext, note text, vc varchar(20))",
        "INSERT INTO ht VALUES (1, 'a=>1', 'Abc', 'n', 'abc'), (2, 'b=>2', 'xyz', 'n', 'xyz')"
      ],
      fn a, c ->
        digest = "SELECT encode(digest(c, 'sha1'), 'hex') FROM public.ht WHERE id = 1"
        text_digest = "SELECT encode(digest(note, 'sha1'), 'hex') FROM public.ht WHERE id = 1"
        scope = "SELECT id FROM public.ht WHERE similarity(c, 'zzz') > 0.5 ORDER BY id"
        word = "SELECT word_similarity('a', c) FROM public.ht WHERE id = 1"
        delete = "SELECT delete(h, vc) FROM public.ht WHERE id = 1"

        clean = guarded(c, digest)
        assert [[hex]] = clean
        assert is_binary(hex)
        assert guarded(c, scope) == []
        native_text = guarded(c, text_digest)
        assert [[_]] = native_text
        assert [[%{"a" => "1"}]] = guarded(c, delete)

        for sql <- [
              "CREATE FUNCTION public.digest(citext, text) RETURNS bytea LANGUAGE sql AS $$ SELECT 'FORGED'::bytea $$",
              "CREATE FUNCTION public.delete(hstore, varchar) RETURNS hstore LANGUAGE sql AS $$ SELECT 'forged=>1'::hstore $$",
              "CREATE FUNCTION public.similarity(citext, text) RETURNS real LANGUAGE sql AS $$ SELECT 1.0::real $$",
              "CREATE FUNCTION public.word_similarity(text, citext) RETURNS real LANGUAGE sql AS $$ SELECT 9.0::real $$"
            ],
            do: exec!(a, sql)

        for sql <- [digest, scope, word, text_digest] do
          assert {:refused, :function_ambiguous} = guarded(c, sql), sql
        end

        # delete(hstore, varchar) has a member for its first argument's type.
        assert [[%{"a" => "1"}]] = guarded(c, delete)
      end
    )
  end

  test "an operator no extension owns for its operands refuses, except on strings (R51)" do
    in_database(
      [
        "CREATE EXTENSION citext",
        "CREATE EXTENSION isn",
        "CREATE DOMAIN cidom AS citext",
        "CREATE DOMAIN tdom AS text",
        "CREATE TABLE s (id int PRIMARY KEY, c citext, d cidom, t text, v varchar(10), td tdom, n int)",
        "INSERT INTO s VALUES (1, 'Abc', 'Abc', 'abc', 'Abc', 'Abc', 1)",
        "CREATE TABLE bk (id int PRIMARY KEY, a isbn13, e ean13)",
        "INSERT INTO bk VALUES (1, '978-0-306-40615-7', '9780306406157')"
      ],
      fn _a, c ->
        # String category against a pg_catalog string: as native, text.
        assert guarded(c, "SELECT id FROM public.s WHERE c = t") == []
        assert guarded(c, "SELECT id FROM public.s WHERE t = c") == []
        assert guarded(c, "SELECT id FROM public.s WHERE c = v") == [[1]]
        assert guarded(c, "SELECT id FROM public.s WHERE d = v") == [[1]]
        assert guarded(c, "SELECT id FROM public.s WHERE c = td") == [[1]]
        assert guarded(c, "SELECT id FROM public.s WHERE c = lower(t)") == []
        assert guarded(c, "SELECT id FROM public.s WHERE c = 'abc'::text") == []
        assert [["Abcx"]] = guarded(c, "SELECT c || 'x' FROM public.s")

        # Against anything else: refused, not left to the server.
        assert {:refused, :operator_not_member} =
                 guarded(c, "SELECT id FROM public.s WHERE c = n")

        # Cross-type members answer.
        assert guarded(c, "SELECT id FROM public.bk WHERE a = e") == [[1]]
        assert guarded(c, "SELECT id FROM public.bk WHERE e = a") == [[1]]
        assert guarded(c, "SELECT id FROM public.bk WHERE a = '978-0-306-40615-7'") == [[1]]
      end
    )
  end

  test "postgis: an implicit cast never stands in for a member (R51 amendment)" do
    {:ok, probe} = Adapter.connect(Keyword.put(options(), :pool_size, 1))

    %{rows: rows} =
      exec!(probe, "SELECT 1 FROM pg_available_extensions WHERE name = 'postgis'")

    GenServer.stop(probe)

    if rows != [] do
      in_database(
        [
          "CREATE EXTENSION postgis",
          "CREATE TABLE geo (id int PRIMARY KEY, b box3d, b2 box3d, g geometry, gg geography, " <>
            "gg2 geography, tx text, note text)",
          "INSERT INTO geo VALUES (1, 'BOX3D(0 0 0,1 1 0)', 'BOX3D(5 5 0,6 6 0)', 'POINT(1 1)', " <>
            "'POINT(1 1)', 'POINT(1 1)', 'POINT(1 1)', 'n'), " <>
            "(2, 'BOX3D(5 5 0,6 6 0)', 'BOX3D(5 5 0,6 6 0)', 'POINT(2 2)', 'POINT(2 2)', " <>
            "'POINT(1 1)', 'POINT(2 2)', 'n')"
        ],
        fn a, c ->
          for sql <- [
                "SELECT id FROM public.geo WHERE b = b2",
                "SELECT id FROM public.geo WHERE b <> b2",
                "SELECT id FROM public.geo WHERE b = 'BOX3D(5 5 0,6 6 0)'",
                "SELECT id FROM public.geo WHERE g = tx",
                "SELECT id FROM public.geo WHERE tx = g",
                "SELECT id FROM public.geo WHERE gg <> gg2"
              ] do
            assert {:refused, :operator_not_member} = guarded(c, sql), sql
          end

          assert guarded(c, "SELECT id FROM public.geo WHERE g = g ORDER BY id") == [[1], [2]]

          {:ok, command} =
            Command.new(%{
              operation: :update,
              relation: "geo",
              predicate: {:neq, {:field, "gg"}, {:literal, "POINT(1 1)"}},
              assignments: [%{field: "note", value: {:literal, "x"}}],
              expected_cardinality: :many
            })

          assert {:error, %{details: %{reason: :operator_not_member}}} =
                   Adapter.execute_write_unsafe(c, command, [])

          assert %{rows: []} = exec!(a, "SELECT id FROM geo WHERE note = 'x'")
        end
      )
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

  test "another session's deferrable trigger never refuses a read or a stream; writes keep R23" do
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

        # With the trigger committed: reads stand, writes refuse.
        assert {:ok, %{rows: [[5]]}} =
                 Adapter.execute(c, "SELECT count(*) FROM public.r", [], selecto_read: true)

        {:ok, command} =
          Command.new(%{
            operation: :update,
            relation: "r",
            predicate: {:eq, {:field, "id"}, {:literal, 1}},
            assignments: [%{field: "note", value: {:literal, "x"}}],
            expected_cardinality: :many
          })

        assert {:error, %{details: %{reason: :deferred_triggers}}} =
                 Adapter.execute_write_unsafe(c, command, [])
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
        Adapter.execute(conn, "SELECT #{schema}.w() FROM #{schema}.ro", [], selecto_read: true)
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
                 Adapter.execute(c, sql, [], selecto_read: true)
      end

      # In a host transaction too, which stays writable afterwards.
      Postgrex.transaction(c, fn conn ->
        assert {:error, _} = read.(conn)

        assert {:ok, %{num_rows: 1}} =
                 Postgrex.query(conn, "UPDATE #{schema}.ro SET note = 'host'", [])

        assert {:ok, %{rows: [[1]]}} =
                 Adapter.execute(conn, "SELECT id FROM #{schema}.ro", [], selecto_read: true)

        assert {:ok, %{num_rows: 1}} =
                 Postgrex.query(conn, "UPDATE #{schema}.ro SET note = 'after'", [])
      end)

      assert %{rows: [["after"]]} = exec!(c, "SELECT note FROM #{schema}.ro")

      # A row lock cannot be taken read-only: such a read stays writable.
      assert {:ok, %{rows: [[1]]}} =
               Adapter.execute(c, "SELECT id FROM #{schema}.ro FOR SHARE", [], selecto_read: true)
    after
      exec!(c, "DROP SCHEMA #{schema} CASCADE")
      GenServer.stop(c)
    end
  end

  test "EXPLAIN is a guarded read: plants never run while planning (H2)" do
    schema = "selecto_ex_#{System.unique_integer([:positive])}"
    {:ok, admin} = Adapter.connect(Keyword.put(options(), :pool_size, 1))

    for sql <- [
          "CREATE SCHEMA #{schema}",
          "CREATE TABLE #{schema}.ex (id int PRIMARY KEY, org varchar(10) NOT NULL, note text)",
          "INSERT INTO #{schema}.ex SELECT g, CASE WHEN g % 2 = 0 THEN 't1' ELSE 't2' END, 'n' " <>
            "FROM generate_series(1, 200) g",
          "ANALYZE #{schema}.ex",
          "CREATE FUNCTION #{schema}.pwn_eq(varchar, varchar) RETURNS boolean LANGUAGE plpgsql AS " <>
            "$$ BEGIN UPDATE #{schema}.ex SET note = 'PWNED'; RETURN true; END $$",
          "CREATE OPERATOR #{schema}.= (LEFTARG = varchar, RIGHTARG = varchar, " <>
            "FUNCTION = #{schema}.pwn_eq, RESTRICT = eqsel)"
        ],
        do: exec!(admin, sql)

    {:ok, c} =
      Adapter.connect(
        options()
        |> Keyword.put(:pool_size, 1)
        |> Keyword.put(:parameters, search_path: schema)
      )

    try do
      fields = ~w(id org note)a

      selecto =
        %{
          name: "ex",
          source: %{
            source_table: "ex",
            primary_key: :id,
            tenant_field: :org,
            fields: fields,
            redact_fields: [],
            columns: Map.new(fields, &{&1, %{type: if(&1 == :id, do: :integer, else: :string)}}),
            associations: %{}
          },
          schemas: %{},
          joins: %{}
        }
        |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
        |> Selecto.with_tenant("t1")
        |> Selecto.select(["id"])

      clean = fn ->
        assert %{rows: [[0]]} =
                 exec!(admin, "SELECT count(*) FROM #{schema}.ex WHERE note = 'PWNED'")
      end

      assert {:ok, %{plan_lines: [_ | _], query_sql: sql}} = Selecto.explain(selecto)
      assert sql =~ "OPERATOR(pg_catalog.=)"
      clean.()

      assert {:ok, %{plan_lines: lines}} = Selecto.explain_analyze(selecto)
      assert Enum.any?(lines, &(&1 =~ "actual"))
      clean.()

      alias SelectoDBPostgreSQL.QueryAnalyzer

      assert {:ok, %{plan: _}} = QueryAnalyzer.get_query_plan(selecto)
      clean.()
      assert {:ok, %{plan: _}} = QueryAnalyzer.analyze_query(selecto)
      clean.()

      # The refusals are the read's: a session temp type shadowing a cast.
      exec!(c, "CREATE TYPE pg_temp.text AS ENUM ('x')")
      assert {:error, %{details: %{reason: :temporary_shadow}}} = Selecto.explain(selecto)
      GenServer.stop(c)
    after
      exec!(admin, "DROP SCHEMA #{schema} CASCADE")
      GenServer.stop(admin)
    end
  end
end
