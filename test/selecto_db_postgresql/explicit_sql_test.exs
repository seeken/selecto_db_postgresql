defmodule SelectoDBPostgreSQL.ExplicitSQLTest do
  # R46 FINAL-2 / R47 / R48 / R49c / R50: the last step before a Selecto
  # statement runs (SelectoDBPostgreSQL.Qualify), on catalog facts given
  # here; the live poison suite is ExplicitPoisonIntegrationTest.
  use ExUnit.Case, async: true

  alias SelectoDBPostgreSQL.{ExplicitSQL, Qualify}

  @catalog ~w(lower upper count sum max min now length replace concat regexp_match unnest
              like_escape coalesce numeric varchar substring overlay left right)

  defp candidate(schema, overrides \\ %{}) do
    Map.merge(
      %{
        schema: schema,
        nargs: 1,
        defaults: 0,
        args: [],
        variadic: nil,
        member: false,
        extension: nil
      },
      overrides
    )
  end

  defp functions(extra \\ %{}) do
    @catalog
    |> Map.new(fn name ->
      {name,
       %{
         candidates: [candidate("pg_catalog", %{nargs: 0, variadic: "pg_catalog.\"any\""})],
         polymorphic: name in ~w(max min coalesce)
       }}
    end)
    |> Map.merge(extra)
  end

  defp qualify!(sql, facts \\ %{}) do
    facts = Map.merge(%{functions: functions(), extension: false}, facts)
    assert {:ok, out} = Qualify.qualify(sql, facts)
    assert Qualify.lint(out) == [], out
    out
  end

  defp citext(fns \\ %{}) do
    %{
      ops: Map.new(~w(= <> < <= > >= ~~ !~~ ~~* !~~*), &{&1, ~s("ext")}),
      fns: fns,
      type: ~s("ext"."citext"),
      elem: nil,
      unknown: false
    }
  end

  defp plain, do: %{ops: %{}, fns: %{}, type: nil, elem: nil, unknown: false}

  defp ext_facts(extra \\ %{}) do
    max = %{schema: "ext", self: true, nargs: 1, args: [~s("ext"."citext")]}

    replace = %{
      schema: "ext",
      self: false,
      nargs: 3,
      args: [~s("ext"."citext"), ~s("ext"."citext"), ~s("ext"."citext")]
    }

    Map.merge(
      %{
        extension: true,
        relations: %{
          "s.t" => %{
            "id" => plain(),
            "name" => plain(),
            "ci" => citext(%{"max" => [max], "replace" => [replace]}),
            "ci2" => citext()
          }
        }
      },
      extra
    )
  end

  describe "operators" do
    test "every bare operator names pg_catalog's, grouped as PostgreSQL groups it" do
      assert qualify!("SELECT 1 FROM s.t WHERE a + 1 * 2 = b AND c || d <> $1") ==
               "SELECT 1 FROM s.t WHERE ((((a) OPERATOR(pg_catalog.+) (((1) OPERATOR(pg_catalog.*) (2))))) " <>
                 "OPERATOR(pg_catalog.=) (b)) AND ((((c) OPERATOR(pg_catalog.||) (d))) OPERATOR(pg_catalog.<>) ($1))"
    end

    test "prefix operators, != and explicit operators" do
      assert qualify!("SELECT -a, b != 1 FROM s.t") ==
               "SELECT (OPERATOR(pg_catalog.-) (a)) , ((b) OPERATOR(pg_catalog.<>) (1)) FROM s.t"

      assert qualify!(~s|SELECT 1 FROM s.t WHERE (a OPERATOR("ext".=) b)|) =~
               ~s|OPERATOR("ext".=)|
    end

    test "IN, BETWEEN, LIKE, SIMILAR TO, IS DISTINCT FROM, NULLIF and simple CASE" do
      for {sql, part} <- [
            {"a IN ($1, $2)",
             "((a) OPERATOR(pg_catalog.=) ($1)) OR ((a) OPERATOR(pg_catalog.=) ($2))"},
            {"a NOT IN ($1)", "(NOT (((a) OPERATOR(pg_catalog.=) ($1))))"},
            {"a IN (SELECT 1)", "((a) OPERATOR(pg_catalog.=) ANY (SELECT 1))"},
            {"a NOT IN (SELECT 1)", "((a) OPERATOR(pg_catalog.<>) ALL (SELECT 1))"},
            {"a BETWEEN 1 AND 2",
             "((a) OPERATOR(pg_catalog.>=) (1) AND (a) OPERATOR(pg_catalog.<=) (2))"},
            {"a LIKE $1", "((a) OPERATOR(pg_catalog.~~) ($1))"},
            {"a NOT ILIKE $1", "((a) OPERATOR(pg_catalog.!~~*) ($1))"},
            {"a LIKE $1 ESCAPE '!'", "pg_catalog.like_escape($1, '!')"},
            {"a SIMILAR TO $1", "pg_catalog.similar_to_escape($1)"},
            {"a IS DISTINCT FROM b", "(NOT (COALESCE((a) OPERATOR(pg_catalog.=) (b), FALSE)"},
            {"a IS NOT DISTINCT FROM b", "(COALESCE((a) OPERATOR(pg_catalog.=) (b), FALSE)"},
            {"NULLIF(a, b) IS NULL",
             "(CASE WHEN (a) OPERATOR(pg_catalog.=) (b) THEN NULL ELSE (a) END)"},
            {"CASE a WHEN 1 THEN true ELSE false END",
             "CASE WHEN (a) OPERATOR(pg_catalog.=) (1) THEN true"}
          ] do
        out = qualify!("SELECT 1 FROM s.t WHERE #{sql}")
        assert String.contains?(out, part), out
      end
    end

    test "assignments, star, window frames, casts and comments are syntax" do
      for sql <- [
            ~s|UPDATE s.t SET "a" = $1, b = (SELECT 2) WHERE ("id" OPERATOR(pg_catalog.=) $2)|,
            "INSERT INTO s.t (a) VALUES ($1) ON CONFLICT (a) DO UPDATE SET a = EXCLUDED.a RETURNING *",
            "SELECT t.*, x::character varying, y::timestamp with time zone FROM s.t",
            "SELECT pg_catalog.sum(a) OVER (ORDER BY b ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM s.t",
            "SELECT EXTRACT(year FROM ts), TRIM(BOTH ' ' FROM c) FROM s.t"
          ] do
        assert Qualify.lint(sql) == [], sql
        assert {:ok, out} = Qualify.qualify(sql, %{functions: functions(), extension: false})
        assert Qualify.lint(out) == [], out
      end

      assert Qualify.lint("/* hint */ SELECT 1 FROM s.t") == []

      assert qualify!("SELECT POSITION('a' IN (b)) FROM s.t") =~
               "pg_catalog.position((b), 'a')"
    end
  end

  describe "functions" do
    test "pg_catalog's are qualified whatever is planted beside them" do
      planted =
        functions(%{
          "lower" => %{
            candidates: [
              candidate("pg_catalog"),
              candidate("public", %{args: ["pg_catalog.varchar"]})
            ],
            polymorphic: false
          }
        })

      assert qualify!("SELECT lower(a), count(*), LEFT(b, 2) FROM s.t", %{functions: planted}) ==
               "SELECT pg_catalog.lower(a) , pg_catalog.count(*) , pg_catalog.LEFT(b, 2) FROM s.t"
    end

    test "the comma forms of SUBSTRING and OVERLAY are name-resolved; the keyword forms are not" do
      assert qualify!("SELECT substring(a, 1, 2), SUBSTRING(a FROM 1 FOR 2) FROM s.t") ==
               "SELECT pg_catalog.substring(a , 1 , 2) , SUBSTRING(a FROM 1 FOR 2) FROM s.t"
    end

    test "column lists, type modifiers and CTE columns are no calls" do
      sql =
        "WITH c (x, y) AS (SELECT 1, 2) SELECT coalesce(a, b), CAST(x AS numeric(10, 2)), " <>
          "merge_action() FROM c AS t(q) WHERE EXISTS (SELECT 1)"

      assert Enum.filter(Qualify.lint(sql), &match?({:function, _}, &1)) == []
    end

    test "an extension-only function: its one candidate, arguments cast to its types (R50)" do
      facts = %{
        functions:
          functions(%{
            "similarity" => %{
              candidates: [
                candidate("ext", %{
                  nargs: 2,
                  args: ["pg_catalog.text", "pg_catalog.text"],
                  member: false
                })
              ],
              polymorphic: false
            }
          })
      }

      assert qualify!("SELECT similarity(a, $1) FROM s.t", facts) =~
               ~s|"ext"."similarity"(CAST((a) AS pg_catalog.text), CAST(($1) AS pg_catalog.text))|
    end

    test "VARIADIC: fixed arguments cast, the rest passed as VARIADIC ARRAY" do
      facts = %{
        functions:
          functions(%{
            "pick" => %{
              candidates: [
                candidate("ext", %{
                  nargs: 2,
                  args: ["pg_catalog.int4", "pg_catalog._text"],
                  variadic: "pg_catalog.text"
                })
              ],
              polymorphic: false
            }
          })
      }

      assert qualify!("SELECT pick(1, a, b) FROM s.t", facts) =~
               ~s|CAST((1) AS pg_catalog.int4), VARIADIC ARRAY[CAST((a) AS pg_catalog.text), CAST((b) AS pg_catalog.text)]|
    end

    test "no candidate, or two beside no catalog function, refuses" do
      assert {:error, :function_not_found, _} =
               Qualify.qualify("SELECT nosuch(a) FROM s.t", %{functions: %{}, extension: false})

      two = %{
        "f" => %{
          candidates: [
            candidate("a", %{args: ["pg_catalog.text"]}),
            candidate("b", %{args: ["pg_catalog.int4"]})
          ],
          polymorphic: false
        }
      }

      assert {:error, :function_ambiguous, _} =
               Qualify.qualify("SELECT f(a) FROM s.t", %{functions: two, extension: false})
    end

    test "a member function beside an unrelated plant is ambiguous" do
      two = %{
        "regexp_split_to_table" => %{
          candidates: [
            candidate("ext", %{member: true, extension: "1", args: [~s("ext"."citext")]}),
            candidate("public", %{args: ["pg_catalog.text"]})
          ],
          polymorphic: false
        }
      }

      assert {:error, :function_ambiguous, _} =
               Qualify.qualify("SELECT 1 FROM regexp_split_to_table(a) AS x", %{
                 functions: two,
                 extension: false
               })
    end
  end

  describe "extension types" do
    test "both citext, or citext and untyped: its operator, operands cast to it (R48, R50)" do
      assert qualify!(~s|SELECT 1 FROM s.t r WHERE r.ci = $1|, ext_facts()) =~
               ~s|(CAST((r.ci) AS "ext"."citext")) OPERATOR("ext".=) (CAST(($1) AS "ext"."citext"))|

      assert qualify!(~s|SELECT 1 FROM s.t r WHERE r.ci = r.ci2|, ext_facts()) =~
               ~s|OPERATOR("ext".=)|
    end

    test "citext against text is pg_catalog's, whatever the operand order" do
      assert qualify!("SELECT 1 FROM s.t r WHERE r.ci = r.name", ext_facts()) =~
               "OPERATOR(pg_catalog.=)"

      assert qualify!("SELECT 1 FROM s.t r WHERE r.name = r.ci", ext_facts()) =~
               "OPERATOR(pg_catalog.=)"
    end

    test "one operator's casts never leak into the next (citext UPDATEs)" do
      out =
        qualify!("SELECT 1 FROM s.t r WHERE r.ci = $1 AND r.id = $2 AND r.name = $3", ext_facts())

      assert out =~ "((r.id) OPERATOR(pg_catalog.=) ($2))"
      assert out =~ "((r.name) OPERATOR(pg_catalog.=) ($3))"
    end

    test "an aggregate over citext is the extension's, typed as citext (R49c, R50)" do
      out =
        qualify!(
          "SELECT 1 FROM s.t r GROUP BY r.id HAVING max(r.ci) = $1",
          ext_facts(%{functions: functions()})
        )

      assert out =~ ~s|"ext".max(CAST((r.ci) AS "ext"."citext"))|
      assert out =~ ~s|OPERATOR("ext".=)|
    end

    test "a citext overload with untyped arguments; a text argument keeps pg_catalog's" do
      assert qualify!("SELECT replace(r.ci, $1, 'x') FROM s.t r", ext_facts()) =~
               ~s|"ext".replace(CAST((r.ci) AS "ext"."citext"), CAST(($1) AS "ext"."citext")|

      assert qualify!("SELECT replace(r.ci, r.name, 'x') FROM s.t r", ext_facts()) =~
               "pg_catalog.replace("
    end

    test "an expression that may be citext against an untyped value refuses" do
      assert {:error, :operand_type_unknown, _} =
               Qualify.qualify(
                 "SELECT 1 FROM s.t r WHERE COALESCE(r.ci, 'x') = $1",
                 Map.put(ext_facts(), :functions, functions())
               )

      assert {:error, :operand_type_unknown, _} =
               Qualify.qualify(
                 "SELECT 1 FROM s.t r WHERE r.ci = ANY($1)",
                 Map.put(ext_facts(), :functions, functions())
               )
    end

    test "citext passes through CTEs and derived tables; a reused alias is its own scope's" do
      out =
        qualify!(
          ~s|WITH m AS (SELECT r.ci AS c FROM s.t r) SELECT 1 FROM m r WHERE r.c = $1|,
          ext_facts()
        )

      assert out =~ ~s|OPERATOR("ext".=)|

      out =
        qualify!(
          ~s|SELECT 1 FROM (SELECT r.name AS ci FROM s.t r) r WHERE r.ci = $1|,
          ext_facts()
        )

      assert out =~ "OPERATOR(pg_catalog.=)"
    end

    test "a cast to the member type gives its operators" do
      facts = Map.put(ext_facts(), :casts, %{"ext.citext" => citext()})

      assert qualify!(~s|SELECT 1 FROM s.t r WHERE CAST(r.name AS "ext"."citext") = $1|, facts) =~
               ~s|OPERATOR("ext".=)|
    end
  end

  describe "prepare" do
    test "an explicit statement reads no catalog" do
      sql = ~s|SELECT pg_catalog.lower(a) FROM s.t WHERE (a OPERATOR(pg_catalog.=) $1)|
      assert {:ok, ^sql} = ExplicitSQL.prepare(fn _, _ -> flunk("no lookup") end, sql)
    end

    test "catalog facts that cannot be read refuse" do
      assert {:error, %{details: %{reason: :function_lookup_unavailable}}} =
               ExplicitSQL.prepare(
                 fn _, _ -> {:error, :down} end,
                 "SELECT 1 FROM s.t WHERE a = 1"
               )
    end

    test "SQL that cannot be read refuses" do
      assert {:error, %{details: %{reason: :sql_unqualifiable}}} =
               ExplicitSQL.prepare(fn _, _ -> {:error, :down} end, "SELECT 'open")
    end
  end

  describe "lint" do
    test "any bare operator, by-name construct or unqualified function" do
      assert Qualify.lint("SELECT 1 WHERE (2 & 3) <> 0") ==
               [{:operator, "&"}, {:operator, "<>"}]

      assert Qualify.lint("SELECT lower(a) FROM t") == [{:function, "lower"}]
      assert Qualify.lint("SELECT 1 FROM t WHERE a IN (1)") == [{:construct, "IN"}]
      assert Qualify.lint(~s|LOCK TABLE "s"."t" IN ROW EXCLUSIVE MODE|) == []
      assert Qualify.lint("SELECT substring(a, 1, 2) FROM t") == [{:function, "substring"}]
    end
  end
end
