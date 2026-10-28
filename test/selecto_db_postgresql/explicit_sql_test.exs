defmodule SelectoDBPostgreSQL.ExplicitSQLTest do
  # R46 FINAL-2 / R47: the last step before a Selecto statement runs.
  use ExUnit.Case, async: true

  alias SelectoDBPostgreSQL.ExplicitSQL

  defp lookup(schemas) do
    fn _sql, [names] ->
      {:ok, %{rows: Enum.map(names, &[&1, Map.get(schemas, &1)])}}
    end
  end

  test "functions are qualified with the schema the pinned path resolves them to" do
    sql = "SELECT lower(a), count(*), similarity(a, $1), pg_catalog.upper(b) FROM t"

    assert {:ok, prepared} =
             ExplicitSQL.prepare(
               lookup(%{"lower" => "pg_catalog", "count" => "pg_catalog", "similarity" => "ext"}),
               sql
             )

    assert prepared ==
             ~s|SELECT pg_catalog.lower(a), pg_catalog.count(*), "ext".similarity(a, $1), pg_catalog.upper(b) FROM t|
  end

  test "syntax, column lists and type modifiers are not function calls" do
    sql =
      "WITH c (x, y) AS (SELECT 1, 2) SELECT coalesce(a, b), CAST(x AS numeric(10, 2)), " <>
        "extract(year FROM ts), position('a' IN 'b'), merge_action() FROM c AS t(q) WHERE EXISTS (SELECT 1)"

    assert ExplicitSQL.function_calls(sql) == []
  end

  test "an unresolvable function or a bare compound comparison refuses" do
    assert {:error, %{details: %{reason: :bare_operator}}} =
             ExplicitSQL.prepare(lookup(%{}), "SELECT nosuch(a) FROM t")

    for sql <- [
          "SELECT 1 FROM t WHERE a + 1 = b",
          "SELECT 1 FROM t WHERE a IN ($1, $2)",
          "SELECT 1 FROM t WHERE a LIKE $1",
          "SELECT 1 FROM t WHERE a BETWEEN $1 AND $2",
          "SELECT 1 FROM t WHERE a IS DISTINCT FROM b"
        ] do
      assert {:error, %{details: %{reason: :bare_operator}}} =
               ExplicitSQL.prepare(lookup(%{}), sql),
             sql
    end
  end

  test "a comparison of two atoms is rewritten to pg_catalog's operator" do
    sql =
      "SELECT 1 FROM n JOIN t t ON t.id = n.parent_id WHERE t.depth < 100 AND x.a != $1 AND k = TRUE"

    assert {:ok, prepared} = ExplicitSQL.prepare(lookup(%{}), sql)

    assert prepared ==
             "SELECT 1 FROM n JOIN t t ON t.id OPERATOR(pg_catalog.=) n.parent_id WHERE " <>
               "t.depth OPERATOR(pg_catalog.<) 100 AND x.a OPERATOR(pg_catalog.<>) $1 AND " <>
               "k OPERATOR(pg_catalog.=) TRUE"
  end

  test "assignments, OPERATOR groups and LOCK modes are not comparisons" do
    for sql <- [
          "UPDATE x SET a = $1, b = (SELECT 2) WHERE (id OPERATOR(pg_catalog.=) $2)",
          ~s|LOCK TABLE "s"."t" IN ROW EXCLUSIVE MODE|,
          "INSERT INTO s.t (a) VALUES ($1) ON CONFLICT (a) DO UPDATE SET a = EXCLUDED.a"
        ] do
      assert {:ok, ^sql} = ExplicitSQL.prepare(lookup(%{}), sql), sql
    end
  end
end
