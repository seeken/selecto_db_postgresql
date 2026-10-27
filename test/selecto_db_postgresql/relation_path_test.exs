defmodule SelectoDBPostgreSQL.RelationPathTest do
  # Relation qualification and the pinned search_path (rulings R23b, R28,
  # R29, R30, R33, R34).
  use ExUnit.Case, async: true

  alias SelectoDBPostgreSQL.{RelationPath, SessionGuard}

  @map %{"c_items" => ~s("app"."c_items"), "c_root" => ~s("app"."c_root")}

  test "a CTE named like the relation it reads qualifies that relation in its own body (R33)" do
    sql = """
    WITH c_items (id) AS (select cte_c_items.id from c_items cte_c_items where cte_c_items.org_key = $1)
    select selecto_root.id from c_root selecto_root inner join c_items c_items on c_items.id = selecto_root.id
    """

    assert {:ok, qualified} = RelationPath.qualify(sql, @map)
    assert qualified =~ ~s|from "app"."c_items" cte_c_items|
    assert qualified =~ ~s|from "app"."c_root" selecto_root|
    # The main query's c_items is the CTE.
    assert qualified =~ "inner join c_items c_items"
  end

  test "an earlier sibling sees the real relation, a later one the CTE (R33)" do
    sql =
      "WITH a AS (select 1 from c_items), c_items AS (select 1 from c_root), " <>
        "b AS (select 1 from c_items) select 1 from b"

    assert {:ok, qualified} = RelationPath.qualify(sql, @map)
    assert qualified =~ ~s|WITH a AS (select 1 from "app"."c_items")|
    assert qualified =~ ~s|c_items AS (select 1 from "app"."c_root")|
    assert qualified =~ "b AS (select 1 from c_items)"
  end

  test "a recursive or nested CTE named like a relation refuses (R33)" do
    for sql <- [
          "WITH RECURSIVE c_items AS (select 1 from c_items) select 1 from c_items",
          "select 1 from c_root where exists (WITH c_items AS (select 1) select 1 from c_items)"
        ] do
      assert {:error, %{details: %{reason: :cte_name_collision}}} =
               RelationPath.qualify(sql, @map)
    end
  end

  test "string literals and quoted identifiers do not unbalance a CTE body" do
    sql = ~s|WITH c_items AS (select ')' AS ")" from c_items) select 1 from c_items|
    assert {:ok, qualified} = RelationPath.qualify(sql, @map)
    assert qualified =~ ~s|from "app"."c_items")|
  end

  test "qualified names, function calls and columns after FROM are not relations (R34)" do
    assert RelationPath.names("select extract(year from s0.ts) from c_root s0, unnest($1) u") ==
             ["c_root"]
  end

  test "LATERAL and ONLY are keywords, not relations" do
    sql =
      "select 1 from c_root r CROSS JOIN LATERAL UNNEST(r.tags) WITH ORDINALITY AS t(v, p) " <>
        "join lateral (select 1) x on true join only c_items i on true"

    assert RelationPath.names(sql) == ["c_root", "c_items"]
    assert {:ok, qualified} = RelationPath.qualify(sql, @map)
    assert qualified =~ ~s|JOIN LATERAL UNNEST(|
    assert qualified =~ ~s|from "app"."c_root" r|
    assert :ok = RelationPath.qualified(qualified)
  end

  test "the pinned path puts pg_catalog first and pg_temp last (R30)" do
    # R43: every schema of the reset path (an extension's `ext` included),
    # then the declared ones, pg_temp last.
    assert RelationPath.pinned_path(%{
             "a" => ~s("app"."a"),
             "b" => ~s("pg_temp_3"."b"),
             RelationPath.path_key() => ["public", "ext"]
           }) == ~s|pg_catalog, "public", "ext", "app", pg_temp|

    assert RelationPath.pinned_path(nil) == "pg_catalog, pg_temp"
  end

  test "the shadow check reads relation positions only (R28)" do
    assert "items" in SessionGuard.relation_tokens(~s|select note from "app"."items"|)
    refute "note" in SessionGuard.relation_tokens(~s|select note from "app"."items"|)
  end

  # R42: every relation position, not only the token after FROM or JOIN.
  test "comma FROM lists, scalar subqueries and USING lists are relation positions" do
    for {sql, want} <- [
          {~s|SELECT s0.id FROM (SELECT 1) AS d, "items" AS s0|, ["items"]},
          {"SELECT s0.id FROM pg_catalog.pg_am d, items s0, c_root r", ["items", "c_root"]},
          {"SELECT 1, (SELECT count(*) FROM (SELECT 1) z, items WHERE org_key = $1) AS n",
           ["items"]},
          {"DELETE FROM c_root USING items, c_items WHERE c_root.id = items.id",
           ["c_root", "items", "c_items"]},
          {"UPDATE c_root SET n = 1 FROM items i, c_items WHERE true",
           ["c_root", "items", "c_items"]},
          {"SELECT a FROM t1 JOIN t2 USING (id), t3", ["t1", "t2", "t3"]},
          {"MERGE INTO c_root USING items ON true WHEN MATCHED THEN UPDATE SET n = 1",
           ["c_root", "items"]}
        ] do
      assert RelationPath.names(sql) == want, sql
    end
  end

  test "FROM inside functions, IS DISTINCT FROM, conflict and locking clauses are not relations" do
    sql =
      "SELECT extract(year FROM ts), substring(note FROM 2), trim(both 'x' FROM note) " <>
        "FROM c_root WHERE a IS DISTINCT FROM $1 AND b = 'from items' " <>
        "-- FROM commented\n FOR UPDATE"

    assert RelationPath.names(sql) == ["c_root"]

    insert =
      "INSERT INTO c_items (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id " <>
        "RETURNING id"

    assert RelationPath.names(insert) == ["c_items"]
  end

  test "comma-list and subquery relations are qualified in place" do
    sql = "SELECT 1 FROM c_root r, c_items i WHERE i.id IN (SELECT id FROM c_items)"
    assert {:ok, qualified} = RelationPath.qualify(sql, @map)

    assert qualified ==
             ~s|SELECT 1 FROM "app"."c_root" r, "app"."c_items" i WHERE i.id IN (SELECT id FROM "app"."c_items")|

    assert :ok = RelationPath.qualified(qualified)
    assert {:error, _} = RelationPath.qualified(sql)
  end
end
