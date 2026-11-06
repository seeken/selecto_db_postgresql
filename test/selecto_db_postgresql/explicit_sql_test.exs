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
      ops:
        Map.new(
          ~w(= <> < <= > >= ~~ !~~ ~~* !~~*),
          &{&1, [%{schema: ~s("ext"), left: ~s("ext"."citext"), right: ~s("ext"."citext")}]}
        ),
      fns: fns,
      type: ~s("ext"."citext"),
      category: "S",
      elem: nil,
      unknown: false
    }
  end

  defp plain, do: %{ops: %{}, fns: %{}, type: nil, elem: nil, unknown: false}

  defp ext_facts(extra \\ %{}) do
    ci = ~s("ext"."citext")
    member = %{member: true, extension: "1", ret: ci}

    extension =
      functions(%{
        "max" => %{
          candidates: [
            candidate("pg_catalog", %{args: ["pg_catalog.text"], ret: "pg_catalog.text"}),
            candidate("ext", Map.merge(member, %{args: [ci]}))
          ],
          polymorphic: true
        },
        "replace" => %{
          candidates: [
            candidate("pg_catalog", %{
              nargs: 3,
              args: ~w(pg_catalog.text pg_catalog.text pg_catalog.text),
              ret: "pg_catalog.text"
            }),
            candidate(
              "ext",
              Map.merge(member, %{nargs: 3, args: [ci, ci, ci], ret: "pg_catalog.text"})
            )
          ],
          polymorphic: false
        }
      })

    Map.merge(
      %{
        extension: true,
        functions: extension,
        implicit: MapSet.new([{"ext.citext", "pg_catalog.text"}]),
        casts: %{"ext.citext" => citext()},
        relations: %{
          "s.t" => %{
            "id" => plain(),
            "name" => %{plain() | type: "pg_catalog.text"},
            "ci" => citext(),
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

      # An operator outside pg_catalog is verified, not trusted.
      assert {:error, :operator_not_member, _} =
               Qualify.qualify(~s|SELECT 1 FROM s.t WHERE (a OPERATOR("ext".=) b)|, %{
                 functions: functions(),
                 extension: false
               })

      assert qualify!(~s|SELECT 1 FROM s.t r WHERE r.ci OPERATOR(ext.=) $1|, ext_facts()) =~
               ~s|OPERATOR("ext".=)|
    end

    test "IN, BETWEEN, LIKE, SIMILAR TO, IS DISTINCT FROM, NULLIF and simple CASE" do
      for {sql, part} <- [
            {"a IN ($1, $2)",
             "((a) OPERATOR(pg_catalog.=) ($1)) OR ((a) OPERATOR(pg_catalog.=) ($2))"},
            {"a NOT IN ($1)", "((a) OPERATOR(pg_catalog.<>) ($1))"},
            {"a NOT IN ($1, $2)", "(NOT (((a) OPERATOR(pg_catalog.=) ($1)) OR"},
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
                  member: true,
                  extension: "1"
                })
              ],
              polymorphic: false
            }
          })
      }

      # `a` has no known type here: left as written, never cast to fit.
      assert qualify!("SELECT similarity(a, $1) FROM s.t", facts) =~
               ~s|"ext"."similarity"(a, CAST(($1) AS pg_catalog.text))|
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
                  variadic: "pg_catalog.text",
                  member: true,
                  extension: "1"
                })
              ],
              polymorphic: false
            }
          })
      }

      assert qualify!("SELECT pick(1, 'a', $1) FROM s.t", facts) =~
               ~s|1, VARIADIC ARRAY[CAST(('a') AS pg_catalog.text), CAST(($1) AS pg_catalog.text)]|
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
               Qualify.qualify("SELECT f(a) FROM s.t", %{
                 functions: two,
                 declared_functions: ["f"],
                 extension: false
               })
    end

    test "a non-member runs only as the domain declared it (R58, R60)" do
      int = "pg_catalog.int4"

      plant = %{
        "f" => %{
          candidates: [
            candidate("public", %{args: [int]}),
            candidate("lib", %{args: ["pg_catalog.int8"]})
          ],
          polymorphic: false
        }
      }

      assert {:error, :function_not_member, _} =
               Qualify.qualify("SELECT f(1::int) FROM s.t", %{functions: plant})

      # A declaration of lib.f never lets public.f answer as the exact match.
      assert {:ok, sql} =
               Qualify.qualify("SELECT f(1::int8) FROM s.t", %{
                 functions: plant,
                 declared_functions: ["lib.f"]
               })

      assert sql =~ ~s("lib"."f"(CAST((1::int8)
    end

    test "a candidate is named only by exact match or as the sole one (R62)" do
      geo = ~s("g"."geography")
      geom = ~s("g"."geometry")
      text = "pg_catalog.text"

      entry = fn type -> %{citext() | type: type, ops: %{}} end

      over = fn args -> candidate("g", %{nargs: 2, args: args, member: true, extension: "2"}) end

      facts = %{
        extension: true,
        functions: %{
          "st_d" => %{
            candidates: [over.([geom, geom]), over.([geo, geo]), over.([text, text])],
            polymorphic: false
          }
        },
        implicit: MapSet.new([{"g.geography", text}, {"g.geometry", text}, {text, "g.geometry"}]),
        casts: %{},
        relations: %{"s.t" => %{"gg" => entry.(geo), "g2" => entry.(geo), "gm" => entry.(geom)}}
      }

      assert qualify!("SELECT st_d(r.gg, r.g2) FROM s.t r", facts) =~
               ~s|"g"."st_d"(CAST((r.gg) AS "g"."geography"), CAST((r.g2) AS "g"."geography"))|

      # The sole candidate that can take them: named, nothing cast to fit it.
      assert qualify!("SELECT st_d(r.gg, r.gm) FROM s.t r", facts) =~ ~s|"g"."st_d"(r.gg, r.gm)|

      assert {:error, :function_ambiguous, _} =
               Qualify.qualify("SELECT st_d('a', 'b') FROM s.t r", facts)
    end

    test "a candidate that cannot take the arguments does not count (R58)" do
      text = "pg_catalog.text"

      facts = %{
        extension: true,
        functions: %{
          "digest" => %{
            candidates: [
              candidate("ext", %{nargs: 2, args: [text, text], extension: "3"}),
              candidate("ext", %{nargs: 2, args: ["pg_catalog.bytea", text], extension: "3"}),
              candidate("public", %{nargs: 2, args: ["pg_catalog.int4", "pg_catalog.int4"]})
            ],
            polymorphic: false
          }
        },
        relations: %{"s.t" => %{"note" => %{plain() | type: text}}}
      }

      assert qualify!("SELECT digest(r.note, 'sha1') FROM s.t r", facts) =~
               ~s|"ext"."digest"(CAST((r.note) AS pg_catalog.text), CAST(('sha1') AS pg_catalog.text))|
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
          ext_facts()
        )

      assert out =~ ~s|"ext"."max"(CAST((r.ci) AS "ext"."citext"))|
      assert out =~ ~s|OPERATOR("ext".=)|
    end

    test "a citext overload with untyped arguments; a text argument keeps pg_catalog's" do
      assert qualify!("SELECT replace(r.ci, $1, 'x') FROM s.t r", ext_facts()) =~
               ~s|"ext"."replace"(CAST((r.ci) AS "ext"."citext"), CAST(($1) AS "ext"."citext")|

      assert qualify!("SELECT replace(r.ci, r.name, 'x') FROM s.t r", ext_facts()) =~
               "pg_catalog.replace("
    end

    test "an expression that may be citext against an untyped value refuses" do
      assert {:error, :operand_type_unknown, _} =
               Qualify.qualify(
                 "SELECT 1 FROM s.t r WHERE COALESCE(r.ci, 'x') = $1",
                 ext_facts()
               )

      assert qualify!(
               "SELECT 1 FROM s.t r WHERE r.ci = ANY($1)",
               ext_facts()
             ) =~ ~s|OPERATOR("ext".=) ANY (CAST(($1) AS "ext"."citext"[]))|
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

  describe "tokens" do
    test "numeric forms stay one token" do
      for literal <- ~w(1_000 0x10 0b11 0o17 1_000.5 1e3 .5) do
        assert Qualify.tokenize(literal) == [{:num, literal}]
      end

      assert qualify!("SELECT 1 FROM s.t WHERE a = 1_000") =~ "(1_000)"
    end

    test "unicode strings and identifiers keep their UESCAPE" do
      assert [{:str, "U&'d!0061' UESCAPE '!'"}] = Qualify.tokenize("U&'d!0061' UESCAPE '!'")
      assert qualify!(~s|SELECT 1 FROM s.t WHERE U&"d\\0061" = 1|) =~ ~s|("da")|
    end

    test "a function is told from syntax by its position, not by its word (R60)" do
      plant = %{
        functions:
          functions(%{
            "cube" => %{
              candidates: [
                candidate("lib", %{args: ["pg_catalog.int4"], member: true, extension: "1"})
              ],
              polymorphic: false
            },
            "explain" => %{
              candidates: [candidate("public", %{args: ["pg_catalog.int4"]})],
              polymorphic: false
            }
          })
      }

      assert qualify!("SELECT cube(1) FROM s.t", plant) =~ ~s("lib"."cube"()
      assert qualify!("SELECT x FROM s.t GROUP BY CUBE (x, y)", plant) =~ "CUBE (x , y)"

      assert {:error, :function_not_member, _} =
               Qualify.qualify(
                 "SELECT explain(1) FROM s.t",
                 Map.merge(%{extension: false}, plant)
               )

      assert Qualify.lint(~s|SELECT cube(1) FROM s.t|) != []
    end

    test "U& identifiers are decoded before qualification and lint (R60)" do
      assert qualify!(~s|SELECT U&"lower"(a) FROM s.t|) =~ ~s(pg_catalog."lower"()
      assert qualify!(~s|SELECT U&"l\\006Fwer"(a) FROM s.t|) =~ ~s(pg_catalog."lower"()
      assert Qualify.lint(~s|SELECT U&"lower"(a) FROM s.t|) != []

      assert {:error, :operator_not_member, _} =
               Qualify.qualify(~s|SELECT 1 FROM s.t WHERE a OPERATOR(U&"public".=) 'x'|, %{
                 functions: functions(),
                 extension: false
               })
    end

    test "LIKE takes a quantifier without parentheses of its own" do
      out = qualify!("SELECT 1 FROM s.t WHERE name LIKE ANY (ARRAY['x%'])")
      assert out =~ "OPERATOR(pg_catalog.~~) ANY"
      refute out =~ "(ANY"
    end

    test "nesting costs a single pass: no typing reads a token twice" do
      case_chain = fn depth ->
        Enum.reduce(1..depth, "'a'", fn n, inner ->
          "CASE WHEN a > #{n} THEN #{inner} ELSE 'b' END"
        end)
      end

      else_chain = fn depth ->
        Enum.reduce(1..depth, "'a'", fn n, inner ->
          "CASE WHEN a > #{n} THEN 'b' ELSE #{inner} END"
        end)
      end

      parens = fn depth ->
        Enum.reduce(1..depth, "'a' || b", fn _n, inner -> "(#{inner} || 'c')" end)
      end

      corpus =
        for name <- ["corpus_nested_1.sql", "corpus_nested_2.sql"],
            do: File.read!(Path.join([__DIR__, "..", "fixtures", name]))

      for sql <-
            corpus ++
              [
                "SELECT 1 FROM s.t WHERE #{case_chain.(200)} = 'a'",
                "SELECT 1 FROM s.t WHERE #{else_chain.(200)} = 'a'",
                "SELECT 1 FROM s.t WHERE #{parens.(200)} = 'a'",
                "SELECT 1 FROM s.t WHERE #{Enum.join(List.duplicate("b", 2000), " || ")} = 'a'"
              ] do
        {micros, _result} = :timer.tc(fn -> Qualify.qualify(sql, %{functions: functions()}) end)
        assert micros < 5_000_000, "#{byte_size(sql)} bytes took #{div(micros, 1000)} ms"
      end
    end

    test "a column named set is not the SET of an UPDATE" do
      out = qualify!("SELECT k.update, k.set = 'other' AS r FROM s.t AS k")
      assert out =~ "OPERATOR(pg_catalog.=)"
    end

    test "a row lock is read from tokens, not from text" do
      lock? = &(&1 |> Qualify.tokenize() |> Qualify.row_lock?())

      for sql <- [
            "SELECT 1 FROM t FOR UPDATE",
            "SELECT 1 FROM t for share",
            "SELECT 1 FROM t FOR NO KEY UPDATE",
            "SELECT 1 FROM t FOR KEY SHARE"
          ],
          do: assert(lock?.(sql), sql)

      for sql <- [
            "SELECT 'x FOR UPDATE' FROM t",
            "SELECT substring(name for share) FROM t",
            ~s|SELECT 1 AS "for share" FROM t|,
            "SELECT 1 /* for update */ FROM t",
            "SELECT 1 -- for no key update\n FROM t"
          ],
          do: refute(lock?.(sql), sql)
    end
  end

  describe "assignments" do
    test "a SET list's first = per item, at its own level, is the assignment" do
      for sql <- [
            ~s|UPDATE s.t SET a = 1, b = CASE WHEN c THEN 2 END, d = 3 WHERE (e OPERATOR(pg_catalog.=) 1)|,
            "UPDATE s.t SET (a, b) = (1, 2) WHERE (e OPERATOR(pg_catalog.=) 1)",
            "UPDATE s.t SET arr[1] = 5 WHERE (e OPERATOR(pg_catalog.=) 1)"
          ] do
        assert Qualify.lint(sql) == [], sql
        assert {:ok, out} = Qualify.qualify(sql, %{functions: functions(), extension: false})
        assert Qualify.lint(out) == [], out
      end

      assert [{:operator, "="}] = Qualify.lint("SELECT id AS set, b = c FROM s.t")
      assert [{:operator, "="}] = Qualify.lint("UPDATE s.t SET a = b = c")
      assert [{:operator, "="}] = Qualify.lint("UPDATE s.t SET a = (b = c)")

      merge =
        "MERGE INTO s.t USING s.u ON (a OPERATOR(pg_catalog.=) b) WHEN MATCHED THEN UPDATE SET c = 1 WHEN MATCHED AND d = 1 THEN DELETE"

      assert [{:operator, "="}] = Qualify.lint(merge)
    end
  end

  describe "lint" do
    test "an operator without a schema, or a foreign one in any case" do
      assert [{:operator, "="}] = Qualify.lint("SELECT 1 FROM s.t WHERE a OPERATOR(=) 1")
      assert qualify!("SELECT 1 FROM s.t WHERE a OPERATOR(=) 1") =~ "OPERATOR(pg_catalog.=)"

      for word <- ~w(OPERATOR operator Operator) do
        assert Qualify.rewrite?(Qualify.tokenize("SELECT a #{word}(public.=) 1"))
      end

      refute Qualify.rewrite?(Qualify.tokenize("SELECT a Operator(pg_catalog.=) 1"))
    end

    test "a function named like a keyword is a function (R60)" do
      for name <- ~w(delete last key update first share) do
        assert [{:function, ^name}] = Qualify.lint("SELECT #{name}(a) FROM s.t")
      end

      assert Qualify.lint("SELECT 1 FROM s.t ORDER BY a NULLS LAST FETCH FIRST (1) ROWS ONLY") ==
               []
    end

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
