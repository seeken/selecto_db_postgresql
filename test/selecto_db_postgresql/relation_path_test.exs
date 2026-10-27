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

  test "the pinned path puts pg_catalog first and pg_temp last (R30)" do
    assert RelationPath.pinned_path(%{"a" => ~s("app"."a"), "b" => ~s("pg_temp_3"."b")}) ==
             ~s|pg_catalog, "app", pg_temp|

    assert RelationPath.pinned_path(nil) == "pg_catalog, pg_temp"
  end

  test "the shadow check reads relation positions only (R28)" do
    assert "items" in SessionGuard.relation_tokens(~s|select note from "app"."items"|)
    refute "note" in SessionGuard.relation_tokens(~s|select note from "app"."items"|)
  end
end
