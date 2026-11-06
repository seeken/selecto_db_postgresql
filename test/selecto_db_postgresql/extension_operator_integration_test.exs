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

      assert {:error, _} = read.(c)
      assert %{rows: [["n"]]} = exec!(c, "SELECT note FROM #{schema}.ro")

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
