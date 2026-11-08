defmodule SelectoDBPostgreSQL.DynamicJoinRelationIntegrationTest do
  # An ad-hoc join's table and a subquery join's tables are relations of the
  # read like the domain's own: resolved against the reset search_path, pinned,
  # and refused where the host's path would read another one (R23b, R59).
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  @moduletag :postgres

  setup do
    opts = Keyword.put(ConnectionOptions.options(), :pool_size, 1)
    {:ok, c} = Adapter.connect(opts)
    Process.unlink(c)
    n = System.unique_integer([:positive])
    {alt, products, reviews} = {"selecto_alt_#{n}", "selecto_dp_#{n}", "selecto_dr_#{n}"}

    for sql <- [
          "CREATE SCHEMA #{alt}",
          "CREATE TABLE public.#{products} (id int PRIMARY KEY, name text)",
          "CREATE TABLE public.#{reviews} (id int PRIMARY KEY, product_id int, rating int, title text)",
          "CREATE TABLE #{alt}.#{products} (id int PRIMARY KEY, name text)",
          "CREATE TABLE #{alt}.#{reviews} (id int PRIMARY KEY, product_id int, rating int, title text)",
          "INSERT INTO public.#{products} VALUES (1, 'a'), (2, 'b')",
          "INSERT INTO public.#{reviews} VALUES (1, 1, 5, 'public')",
          "INSERT INTO #{alt}.#{products} VALUES (1, 'alt-a')",
          "INSERT INTO #{alt}.#{reviews} VALUES (1, 1, 5, 'alt')"
        ],
        do: {:ok, _} = Adapter.execute(c, sql, [], [])

    on_exit(fn ->
      {:ok, d} = Adapter.connect(opts)
      Adapter.execute(d, "DROP SCHEMA IF EXISTS #{alt} CASCADE", [], [])
      Adapter.execute(d, "DROP TABLE IF EXISTS public.#{products}, public.#{reviews}", [], [])
      GenServer.stop(d)
    end)

    %{connection: c, alt: alt, products: products, reviews: reviews}
  end

  defp domain(table, fields, schema \\ nil) do
    source =
      %{
        source_table: table,
        primary_key: :id,
        fields: Keyword.keys(fields),
        redact_fields: [],
        columns: Map.new(fields, fn {name, type} -> {name, %{type: type}} end),
        associations: %{}
      }

    source = if schema, do: Map.put(source, :source_schema, schema), else: source
    %{name: table, source: source, schemas: %{}, joins: %{}}
  end

  defp products(ctx, schema \\ nil),
    do: domain(ctx.products, [id: :integer, name: :string], schema)

  defp reviews(ctx, schema \\ nil),
    do:
      domain(
        ctx.reviews,
        [id: :integer, product_id: :integer, rating: :integer, title: :string],
        schema
      )

  defp ad_hoc(ctx, domain) do
    Selecto.configure(domain, ctx.connection, adapter: Adapter)
    |> Selecto.join(:reviews,
      source: ctx.reviews,
      type: :left,
      owner_key: :id,
      related_key: :product_id,
      fields: %{rating: %{type: :integer}, title: %{type: :string}}
    )
    |> Selecto.select(["name", "reviews.title"])
    |> Selecto.filter({"reviews.rating", {:>=, 4}})
  end

  defp subquery_join(ctx, inner_schema \\ nil) do
    sub =
      Selecto.configure(reviews(ctx, inner_schema), ctx.connection, adapter: Adapter)
      |> Selecto.select(["product_id", "title"])
      |> Selecto.filter({"rating", {:>=, 4}})

    Selecto.configure(products(ctx), ctx.connection, adapter: Adapter)
    |> Selecto.DynamicJoin.join_subquery(:best, sub, on: [%{left: "id", right: "product_id"}])
    |> Selecto.select(["name", "best.title"])
    |> Selecto.order_by("name")
  end

  defp rows({:ok, {rows, _columns, _aliases}}), do: rows
  defp rows({:error, error}), do: {:error, error.details[:reason], error.details[:relations]}

  # Only the reviews table differs under the host's path.
  defp diverging(ctx, fun) do
    {:ok, _} = Adapter.execute(ctx.connection, "DROP TABLE #{ctx.alt}.#{ctx.products}", [], [])
    {:ok, _} = Adapter.execute(ctx.connection, "SET search_path TO #{ctx.alt}, public", [], [])

    try do
      fun.()
    after
      {:ok, _} = Adapter.execute(ctx.connection, "SET search_path TO DEFAULT", [], [])
    end
  end

  test "an ad-hoc join on a public table answers on default settings", ctx do
    assert rows(Selecto.execute(ad_hoc(ctx, products(ctx)))) == [["a", "public"]]
  end

  test "an ad-hoc join on a VALUES list the statement defines answers", ctx do
    query =
      Selecto.configure(products(ctx), ctx.connection, adapter: Adapter)
      |> Selecto.with_values([[1, "first"]], columns: ["product_id", "label"], as: "labels")
      |> Selecto.join(:labels,
        source: "labels",
        type: :left,
        owner_key: :id,
        related_key: :product_id,
        fields: %{label: %{type: :string}}
      )
      |> Selecto.select(["name", "labels.label"])
      |> Selecto.order_by("name")

    assert rows(Selecto.execute(query)) == [["a", "first"], ["b", nil]]
  end

  test "under a root that declares its schema, an ad-hoc join resolves there", ctx do
    assert rows(Selecto.execute(ad_hoc(ctx, products(ctx, ctx.alt)))) == [["alt-a", "alt"]]
  end

  test "under a host path that reads another table, an ad-hoc join refuses", ctx do
    diverging(ctx, fn ->
      assert {:error, :relation_ambiguous, [reviews]} =
               rows(Selecto.execute(ad_hoc(ctx, products(ctx))))

      assert reviews == ctx.reviews
    end)
  end

  test "a subquery join reads the pinned schema, never the host's", ctx do
    assert rows(Selecto.execute(subquery_join(ctx))) == [["a", "public"], ["b", nil]]

    diverging(ctx, fn ->
      assert {:error, :relation_ambiguous, [reviews]} =
               rows(Selecto.execute(subquery_join(ctx)))

      assert reviews == ctx.reviews
    end)
  end

  defp ad_hoc_and_subquery(ctx, outer_schema, inner_schema) do
    sub =
      Selecto.configure(reviews(ctx, inner_schema), ctx.connection, adapter: Adapter)
      |> Selecto.select(["product_id", "title"])

    Selecto.configure(products(ctx, outer_schema), ctx.connection, adapter: Adapter)
    |> Selecto.join(:rv,
      source: ctx.reviews,
      type: :left,
      owner_key: :id,
      related_key: :product_id,
      fields: %{title: %{type: :string}}
    )
    |> Selecto.DynamicJoin.join_subquery(:best, sub, on: [%{left: "id", right: "product_id"}])
    |> Selecto.select(["name", "rv.title", "best.title"])
  end

  defp review_titles(ctx, schema) do
    Selecto.configure(reviews(ctx, schema), ctx.connection, adapter: Adapter)
    |> Selecto.select(["title"])
  end

  defp lateral_reviews(ctx) do
    child = fn _parent ->
      Selecto.configure(reviews(ctx), ctx.connection, adapter: Adapter)
      |> Selecto.select(["title"])
      |> Selecto.filter({"product_id", {:ref, "selecto_root.id"}})
    end

    Selecto.configure(products(ctx), ctx.connection, adapter: Adapter)
    |> Selecto.lateral_join(:left, child, "lat")
    |> Selecto.select(["name", "lat.title"])
    |> Selecto.order_by("name")
  end

  defp cte_reviews(ctx, name) do
    body = fn ->
      Selecto.configure(reviews(ctx), ctx.connection, adapter: Adapter)
      |> Selecto.select(["product_id", "title"])
    end

    Selecto.configure(products(ctx), ctx.connection, adapter: Adapter)
    |> Selecto.with_cte(name, body,
      join: [owner_key: :id, related_key: :product_id, fields: %{title: %{type: :string}}]
    )
    |> Selecto.select(["name", "#{name}.title"])
    |> Selecto.order_by("name")
  end

  test "one name its queries would read in two schemas refuses, whichever declares", ctx do
    # The compiled SQL names the table once; each query resolves under its own declarations.
    for {outer, inner} <- [{ctx.alt, "public"}, {nil, ctx.alt}] do
      assert {:error, :relation_ambiguous, [reviews]} =
               rows(Selecto.execute(ad_hoc_and_subquery(ctx, outer, inner))),
             inspect({outer, inner})

      assert reviews == ctx.reviews
    end

    # The same schema both ways answers; an undeclared subquery takes its enclosing root's.
    assert {:ok, _} = Selecto.execute(ad_hoc_and_subquery(ctx, nil, "public"))

    assert rows(Selecto.execute(ad_hoc_and_subquery(ctx, ctx.alt, nil))) ==
             [["alt-a", "alt", "alt"]]
  end

  test "a nested query declaring no schema takes its enclosing root's (R59)", ctx do
    only = "selecto_only_#{System.unique_integer([:positive])}"

    for sql <- [
          "CREATE TABLE #{ctx.alt}.#{only} (id int PRIMARY KEY, product_id int, rating int, title text)",
          "INSERT INTO #{ctx.alt}.#{only} VALUES (1, 1, 5, 'only-alt')"
        ],
        do: {:ok, _} = Adapter.execute(ctx.connection, sql, [], [])

    inner = fn table ->
      domain = put_in(reviews(ctx), [:source, :source_table], table)

      Selecto.configure(domain, ctx.connection, adapter: Adapter)
      |> Selecto.select(["product_id", "title"])
    end

    for table <- [ctx.reviews, only] do
      query =
        Selecto.configure(products(ctx, ctx.alt), ctx.connection, adapter: Adapter)
        |> Selecto.DynamicJoin.join_subquery(:best, inner.(table),
          on: [%{left: "id", right: "product_id"}]
        )
        |> Selecto.select(["name", "best.title"])

      assert [["alt-a", title]] = rows(Selecto.execute(query)), table
      assert title in ["alt", "only-alt"]
    end
  end

  test "a CTE or VALUES name hides a table only where the SQL can see it", ctx do
    cte_body = fn ->
      Selecto.configure(reviews(ctx), ctx.connection, adapter: Adapter)
      |> Selecto.select(["product_id", "title"])
    end

    alt_body = fn ->
      Selecto.configure(reviews(ctx, ctx.alt), ctx.connection, adapter: Adapter)
      |> Selecto.select(["product_id", "title"])
    end

    # A union member defines a CTE, or VALUES, named like the other member's table.
    for define <- [
          &Selecto.with_cte(&1, ctx.reviews, alt_body),
          &Selecto.with_values(&1, [[1, "V"]], columns: ["product_id", "title"], as: ctx.reviews)
        ] do
      right =
        Selecto.configure(products(ctx), ctx.connection, adapter: Adapter)
        |> define.()
        |> Selecto.select(["name"])

      union = Selecto.union(review_titles(ctx, ctx.alt), right)
      assert {:error, :cte_name_collision, _} = rows(Selecto.execute(union))
    end

    # A subquery's inner query defines a CTE named like the outer root.
    inner =
      Selecto.configure(reviews(ctx), ctx.connection, adapter: Adapter)
      |> Selecto.with_cte(ctx.products, cte_body)
      |> Selecto.select(["product_id", "title"])

    query =
      Selecto.configure(products(ctx, ctx.alt), ctx.connection, adapter: Adapter)
      |> Selecto.DynamicJoin.join_subquery(:best, inner, on: [%{left: "id", right: "product_id"}])
      |> Selecto.select(["name", "best.title"])

    assert {:error, :cte_name_collision, _} = rows(Selecto.execute(query))

    # An earlier CTE's body reads the real table a later CTE is named after.
    two =
      Selecto.configure(products(ctx), ctx.connection, adapter: Adapter)
      |> Selecto.with_cte(
        "c1",
        alt_body,
        join: [owner_key: :id, related_key: :product_id, fields: %{title: %{type: :string}}]
      )
      |> Selecto.with_cte(ctx.reviews, fn ->
        Selecto.configure(products(ctx), ctx.connection, adapter: Adapter)
        |> Selecto.select(["id", "name"])
      end)
      |> Selecto.select(["name", "c1.title"])
      |> Selecto.order_by("name")

    assert rows(Selecto.execute(two)) == [["a", "alt"], ["b", nil]]
  end

  test "union members that read one name in two schemas refuse", ctx do
    for {left, right} <- [{ctx.alt, nil}, {nil, ctx.alt}] do
      union = Selecto.union(review_titles(ctx, left), review_titles(ctx, right))

      assert {:error, :relation_ambiguous, [reviews]} = rows(Selecto.execute(union)),
             inspect({left, right})

      assert reviews == ctx.reviews
    end
  end

  test "a LATERAL join's query and a CTE's query resolve like the outer one's", ctx do
    expected = [["a", "public"], ["b", nil]]
    assert rows(Selecto.execute(lateral_reviews(ctx))) == expected
    assert rows(Selecto.execute(cte_reviews(ctx, "best"))) == expected
    # A CTE named like the table it reads: its body reads the table.
    assert rows(Selecto.execute(cte_reviews(ctx, ctx.reviews))) == expected

    diverging(ctx, fn ->
      for query <- [lateral_reviews(ctx), cte_reviews(ctx, "best"), cte_reviews(ctx, ctx.reviews)] do
        assert {:error, :relation_ambiguous, [reviews]} = rows(Selecto.execute(query))
        assert reviews == ctx.reviews
      end
    end)
  end

  test "a temporary-only table: the refusal names what a host can declare", ctx do
    temp = "selecto_tmp_#{System.unique_integer([:positive])}"

    for sql <- [
          "CREATE TEMP TABLE #{temp} (id int, product_id int, rating int, title text)",
          "INSERT INTO pg_temp.#{temp} VALUES (1, 1, 5, 'temp')"
        ],
        do: {:ok, _} = Adapter.execute(ctx.connection, sql, [], [])

    query = ad_hoc(ctx, products(ctx)) |> then(&put_in(&1.set.dynamic_joins.reviews.source, temp))
    assert {:error, error} = Selecto.execute(query)
    assert error.details.reason == :relation_unresolved
    assert error.message =~ ~s(source_schema: "pg_temp")

    sub =
      Selecto.configure(
        %{reviews(ctx, "pg_temp") | name: temp} |> put_in([:source, :source_table], temp),
        ctx.connection,
        adapter: Adapter
      )
      |> Selecto.select(["product_id", "title"])

    joined =
      Selecto.configure(products(ctx), ctx.connection, adapter: Adapter)
      |> Selecto.DynamicJoin.join_subquery(:best, sub, on: [%{left: "id", right: "product_id"}])
      |> Selecto.select(["name", "best.title"])
      |> Selecto.order_by("name")

    assert rows(Selecto.execute(joined)) == [["a", "temp"], ["b", nil]]

    # Undeclared under a root declaring public, it is looked for in public: refused, typed.
    undeclared =
      Selecto.configure(put_in(reviews(ctx), [:source, :source_table], temp), ctx.connection,
        adapter: Adapter
      )
      |> Selecto.select(["product_id", "title"])

    missing =
      Selecto.configure(products(ctx, "public"), ctx.connection, adapter: Adapter)
      |> Selecto.DynamicJoin.join_subquery(:best, undeclared,
        on: [%{left: "id", right: "product_id"}]
      )
      |> Selecto.select(["name", "best.title"])

    assert {:error, :relation_unresolved, [_temp]} = rows(Selecto.execute(missing))
  end

  defp reviews_to_products(ctx, schema) do
    reviews(ctx, schema)
    |> put_in([:source, :associations], %{
      prod: %{queryable: :prod, field: :prod, owner_key: :product_id, related_key: :id}
    })
    |> Map.put(:schemas, %{prod: products(ctx).source})
    |> Map.put(:joins, %{prod: %{type: :left, name: "prod"}})
  end

  defp liked_products(ctx, schema) do
    Selecto.configure(reviews_to_products(ctx, schema), ctx.connection, adapter: Adapter)
    |> Selecto.filter({"rating", {:>=, 4}})
    |> Selecto.retarget(:prod)
  end

  test "a retargeted query: the origin under the target's WITH, the target under its root", ctx do
    assert rows(Selecto.execute(liked_products(ctx, nil) |> Selecto.select(["name"]))) == [["a"]]

    # The target's VALUES named like the origin's table would feed the origin.
    shadowed =
      liked_products(ctx, nil)
      |> Selecto.with_values([[2, 9, 5, "V"]],
        columns: ["id", "product_id", "rating", "title"],
        as: ctx.reviews
      )
      |> Selecto.select(["name"])

    assert {:error, :cte_name_collision, [reviews]} = rows(Selecto.execute(shadowed))
    assert reviews == ctx.reviews

    # A target declaring no schema takes its origin's declared root (R59).
    assert rows(Selecto.execute(liked_products(ctx, ctx.alt) |> Selecto.select(["name"]))) ==
             [["alt-a"]]
  end

  test "a CTE body's own CTEs are emitted inside it; its VALUES lists stay visible", ctx do
    body = fn own_name ->
      fn ->
        Selecto.configure(reviews(ctx), ctx.connection, adapter: Adapter)
        |> Selecto.with_cte(own_name, fn ->
          Selecto.configure(products(ctx), ctx.connection, adapter: Adapter)
          |> Selecto.select(["id"])
          |> Selecto.filter({"id", 2})
        end)
        |> Selecto.join(:only,
          source: own_name,
          type: :inner,
          owner_key: :product_id,
          related_key: :id
        )
        |> Selecto.select(["product_id", "title"])
      end
    end

    outer = fn builder ->
      Selecto.configure(products(ctx), ctx.connection, adapter: Adapter)
      |> Selecto.with_cte("cb", builder,
        join: [owner_key: :id, related_key: :product_id, fields: %{title: %{type: :string}}]
      )
      |> Selecto.select(["name", "cb.title"])
      |> Selecto.order_by("name")
    end

    # Only product b passes the body's own CTE, and b has no review.
    assert rows(Selecto.execute(outer.(body.("only_b")))) == [["a", nil], ["b", nil]]

    # Named like a table the statement reads, it cannot be scoped.
    assert {:error, :cte_name_collision, _} =
             rows(Selecto.execute(outer.(body.(ctx.products))))

    # A VALUES list of the enclosing query is visible in its CTE bodies.
    labelled = fn ->
      Selecto.configure(reviews(ctx), ctx.connection, adapter: Adapter)
      |> Selecto.join(:vl,
        source: "vlist",
        type: :inner,
        owner_key: :product_id,
        related_key: :product_id,
        fields: %{label: %{type: :string}}
      )
      |> Selecto.select(["product_id", "vl.label"])
    end

    query =
      Selecto.configure(products(ctx), ctx.connection, adapter: Adapter)
      |> Selecto.with_values([[1, "V"]], columns: ["product_id", "label"], as: "vlist")
      |> Selecto.with_cte("cl", labelled,
        join: [owner_key: :id, related_key: :product_id, fields: %{label: %{type: :string}}]
      )
      |> Selecto.select(["name", "cl.label"])
      |> Selecto.order_by("name")

    assert rows(Selecto.execute(query)) == [["a", "V"], ["b", nil]]
  end

  test "a body's copy of a statement CTE emitted after it is the body's own", ctx do
    shadow = "selecto_cl_#{System.unique_integer([:positive])}"

    for sql <- [
          "CREATE TABLE public.#{shadow} (product_id int, label text)",
          "INSERT INTO public.#{shadow} VALUES (1, 'TABLE')"
        ],
        do: {:ok, _} = Adapter.execute(ctx.connection, sql, [], [])

    spec =
      Selecto.Advanced.CTE.create_cte(
        shadow,
        fn ->
          Selecto.configure(products(ctx), ctx.connection, adapter: Adapter)
          |> Selecto.select(["id", "name"])
          |> Selecto.filter({"id", 1})
        end,
        columns: ["product_id", "label"]
      )

    body = fn ->
      Selecto.configure(reviews(ctx), ctx.connection, adapter: Adapter)
      |> Selecto.with_ctes([spec])
      |> Selecto.join(:cj,
        source: shadow,
        type: :left,
        owner_key: :product_id,
        related_key: :product_id,
        fields: %{label: %{type: :string}}
      )
      |> Selecto.select(["id", "cj.label"])
    end

    for order <- [:statement_copy_after, :statement_copy_before] do
      base = Selecto.configure(reviews(ctx), ctx.connection, adapter: Adapter)
      base = if order == :statement_copy_before, do: Selecto.with_ctes(base, [spec]), else: base

      query =
        base
        |> Selecto.with_cte("cb", body,
          columns: ["rid", "clab"],
          join: [owner_key: :id, related_key: :rid, fields: %{clab: %{type: :string}}]
        )
        |> then(&if(order == :statement_copy_after, do: Selecto.with_ctes(&1, [spec]), else: &1))
        |> Selecto.select(["title", "cb.clab"])

      assert rows(Selecto.execute(query)) == [["public", "a"]], inspect(order)
    end
  end

  test "a recursive CTE whose base defines its own CTE refuses, typed", ctx do
    query =
      Selecto.configure(products(ctx), ctx.connection, adapter: Adapter)
      |> Selecto.with_recursive_cte("walk",
        base_query: fn ->
          Selecto.configure(reviews(ctx), ctx.connection, adapter: Adapter)
          |> Selecto.with_cte("zz", fn ->
            Selecto.configure(products(ctx), ctx.connection, adapter: Adapter)
            |> Selecto.select(["id"])
          end)
          |> Selecto.select(["product_id"])
        end,
        recursive_query: fn _ref ->
          Selecto.configure(reviews(ctx), ctx.connection, adapter: Adapter)
          |> Selecto.join(:w,
            source: "walk",
            type: :inner,
            owner_key: :product_id,
            related_key: :product_id,
            fields: %{"product_id" => %{type: :integer}}
          )
          |> Selecto.select(["product_id"])
        end,
        columns: ["product_id"],
        join: [
          owner_key: :id,
          related_key: :product_id,
          fields: %{"product_id" => %{type: :integer}}
        ]
      )
      |> Selecto.select(["name", "walk.product_id"])

    assert {:error, :recursive_cte_nested_with, ["zz"]} = rows(Selecto.execute(query))
  end

  test "a subquery whose domain declares its schema reads there", ctx do
    assert rows(Selecto.execute(subquery_join(ctx, ctx.alt))) == [["a", "alt"], ["b", nil]]
  end
end
