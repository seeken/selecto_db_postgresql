defmodule SelectoDBPostgreSQL.NameRuleIntegrationTest do
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  @moduletag :postgres

  @adapter Adapter

  setup do
    opts = Keyword.put(ConnectionOptions.options(), :pool_size, 1)
    {:ok, c} = Adapter.connect(opts)
    Process.unlink(c)
    n = System.unique_integer([:positive])
    t = %{alt: "nr_alt_#{n}", p: "nr_p_#{n}", r: "nr_r_#{n}", lk: "nr_lk_#{n}"}

    for sql <-
          [
            "CREATE SCHEMA #{t.alt}",
            "CREATE TABLE public.#{t.p} (id INT PRIMARY KEY, name VARCHAR(20), tenant_id INT)",
            "CREATE TABLE public.#{t.r} (id INT PRIMARY KEY, product_id INT, rating INT, title VARCHAR(20), tenant_id INT)",
            "CREATE TABLE public.#{t.lk} (product_id INT, label VARCHAR(20))",
            "INSERT INTO public.#{t.p} VALUES (1, 'a', 1), (2, 'b', 2)",
            "INSERT INTO public.#{t.r} VALUES (1, 1, 5, 'pub-t1', 1), (2, 1, 5, 'pub-t2', 2), (3, 2, 1, 'pub-low', 2)",
            "INSERT INTO public.#{t.lk} VALUES (1, 'TBL-LK')",
            "CREATE TABLE #{t.alt}.#{t.p} (id INT PRIMARY KEY, name VARCHAR(20), tenant_id INT)",
            "CREATE TABLE #{t.alt}.#{t.r} (id INT PRIMARY KEY, product_id INT, rating INT, title VARCHAR(20), tenant_id INT)",
            "CREATE TABLE #{t.alt}.#{t.lk} (product_id INT, label VARCHAR(20))",
            "INSERT INTO #{t.alt}.#{t.p} VALUES (1, 'alt-a', 1), (2, 'alt-b', 2)",
            "INSERT INTO #{t.alt}.#{t.r} VALUES (1, 2, 5, 'alt', 1)",
            "INSERT INTO #{t.alt}.#{t.lk} VALUES (1, 'ALT-LK')"
          ],
        do: {:ok, _} = Adapter.execute(c, sql, [], [])

    on_exit(fn ->
      {:ok, d} = Adapter.connect(opts)
      Adapter.execute(d, "DROP SCHEMA IF EXISTS #{t.alt} CASCADE", [], [])

      Adapter.execute(
        d,
        "DROP TABLE IF EXISTS public.#{t.p}, public.#{t.r}, public.#{t.lk}",
        [],
        []
      )

      GenServer.stop(d)
    end)

    %{conn: c, t: t}
  end

  # Shared shapes: filter subqueries, declared non-association joins, domains
  # rooted on a CTE, and lists private to union members.

  defp dom(table, fields, extra \\ %{}) do
    %{
      name: table,
      schemas: %{},
      joins: %{},
      source:
        Map.merge(
          %{
            source_table: table,
            primary_key: :id,
            fields: Keyword.keys(fields),
            redact_fields: [],
            columns: Map.new(fields, fn {k, v} -> {k, %{type: v}} end),
            associations: %{}
          },
          extra
        )
    }
  end

  @pf [id: :integer, name: :string, tenant_id: :integer]
  @rf [id: :integer, product_id: :integer, rating: :integer, title: :string, tenant_id: :integer]

  defp p(ctx), do: dom(ctx.t.p, @pf)
  defp r(ctx), do: dom(ctx.t.r, @rf)
  defp q(ctx, domain), do: Selecto.configure(domain, ctx.conn, adapter: @adapter, validate: false)

  defp outcome(query) do
    case Selecto.execute(query) do
      {:ok, {rows, _, _}} -> Enum.sort(rows)
      {:error, %Selecto.Error{type: type, details: details}} -> {:error, details[:reason] || type}
    end
  end

  defp refusal(query) do
    case Selecto.execute(query) do
      {:error, %Selecto.Error{details: %{reason: reason, relations: names}}} -> {reason, names}
      other -> other
    end
  end

  # Products with a review rated 4 or more: only product 1.
  defp rated(ctx),
    do: q(ctx, r(ctx)) |> Selecto.select(["product_id"]) |> Selecto.filter({"rating", {:>=, 4}})

  defp values_r(query, ctx),
    do:
      Selecto.with_values(query, [[2, 5, 99, "VALS", 1]],
        columns: ["product_id", "rating", "id", "title", "tenant_id"],
        as: ctx.t.r
      )

  test "a filter subquery reads its table; a list named like it refuses", ctx do
    in_filter = q(ctx, p(ctx)) |> Selecto.filter({"id", {:subquery, :in, rated(ctx)}})
    assert outcome(Selecto.select(in_filter, ["name"])) == [["a"]]

    assert refusal(in_filter |> values_r(ctx) |> Selecto.select(["name"])) ==
             {:cte_name_collision, [ctx.t.r]}

    exists =
      q(ctx, r(ctx))
      |> Selecto.select(["id"])
      |> Selecto.filter({"product_id", 2})
      |> Selecto.filter({"rating", {:>=, 4}})

    exists_query = q(ctx, p(ctx)) |> Selecto.filter({:exists, exists})
    assert outcome(Selecto.select(exists_query, ["name"])) == []

    assert refusal(exists_query |> values_r(ctx) |> Selecto.select(["name"])) ==
             {:cte_name_collision, [ctx.t.r]}

    any =
      q(ctx, p(ctx))
      |> values_r(ctx)
      |> Selecto.filter({"id", "=", {:subquery, :any, rated(ctx)}})
      |> Selecto.select(["name"])

    assert refusal(any) == {:cte_name_collision, [ctx.t.r]}
  end

  test "a declared non-association join reads its table; a list named like it refuses", ctx do
    domain =
      put_in(p(ctx), [:joins], %{
        lkd: %{
          non_assoc: true,
          source: ctx.t.lk,
          name: "lkd",
          type: :left,
          owner_key: :id,
          related_key: :product_id,
          fields: %{"label" => %{type: :string}}
        }
      })

    query = q(ctx, domain) |> Selecto.select(["name", "lkd.label"])
    assert outcome(query) == [["a", "TBL-LK"], ["b", nil]]

    named =
      Selecto.with_values(query, [[1, "OUTER-V"]], columns: ["product_id", "label"], as: ctx.t.lk)

    assert refusal(named) == {:cte_name_collision, [ctx.t.lk]}
  end

  test "a domain rooted on a CTE reads the CTE", ctx do
    one = fn -> q(ctx, p(ctx)) |> Selecto.select(["id", "name"]) |> Selecto.filter({"id", 1}) end
    c1 = dom("cte_one", id: :integer, name: :string)
    c2 = dom("cte_two", id: :integer, name: :string)

    rooted = q(ctx, c1) |> Selecto.with_cte("cte_one", one, columns: ["id", "name"])
    assert outcome(Selecto.select(rooted, ["name"])) == [["a"]]

    chained =
      q(ctx, c2)
      |> Selecto.with_cte("cte_one", one, columns: ["id", "name"])
      |> Selecto.with_cte("cte_two", fn -> q(ctx, c1) |> Selecto.select(["id", "name"]) end,
        columns: ["id", "name"],
        dependencies: ["cte_one"]
      )

    assert outcome(Selecto.select(chained, ["name"])) == [["a"]]

    # The same name read as a table elsewhere in the statement still refuses.
    as_table = dom(ctx.t.lk, product_id: :integer, label: :string)
    lk_rows = fn -> q(ctx, p(ctx)) |> Selecto.select(["id", "name"]) end

    mixed =
      q(ctx, dom(ctx.t.lk, id: :integer, name: :string))
      |> Selecto.with_cte(
        "cb",
        fn -> q(ctx, as_table) |> Selecto.select(["product_id", "label"]) end,
        columns: ["pid", "plabel"]
      )
      |> Selecto.with_cte(ctx.t.lk, lk_rows, columns: ["id", "name"])
      |> Selecto.select(["name"])

    assert refusal(mixed) == {:cte_name_collision, [ctx.t.lk]}
  end

  defp labelled(ctx, label) do
    q(ctx, p(ctx))
    |> Selecto.with_values([[1, label]], columns: ["product_id", "label"], as: "vv")
    |> Selecto.join(:vj,
      source: "vv",
      type: :left,
      owner_key: :id,
      related_key: :product_id,
      fields: %{"label" => %{type: :string}}
    )
    |> Selecto.select(["name", "vj.label"])
  end

  test "different lists of one name in two union members are private to each", ctx do
    union = Selecto.union(labelled(ctx, "L"), labelled(ctx, "R"), all: true)
    assert outcome(union) == [["a", "L"], ["a", "R"], ["b", nil], ["b", nil]]

    # A member's list and a different statement list of that name refuse.
    body = fn -> Selecto.union(labelled(ctx, "L"), labelled(ctx, "L"), all: true) end

    statement =
      q(ctx, p(ctx))
      |> Selecto.with_values([[2, "MAIN"]], columns: ["product_id", "label"], as: "vv")
      |> Selecto.with_cte("cb", body, columns: ["cname", "clab"])
      |> Selecto.select(["name"])

    assert refusal(statement) == {:cte_name_redefined, ["vv"]}
  end

  defp alt(domain, ctx), do: put_in(domain, [:source, :source_schema], ctx.t.alt)

  test "a filter subquery resolves under its own declaration, or the root's", ctx do
    # The subquery declares alt, the outer query reads public r: one name, two
    # relations in one statement, refused rather than read one for both.
    inner = q(ctx, alt(r(ctx), ctx)) |> Selecto.select(["product_id"])

    both =
      q(ctx, r(ctx))
      |> Selecto.filter({"product_id", {:subquery, :in, inner}})
      |> Selecto.select(["title"])

    assert refusal(both) == {:relation_ambiguous, [ctx.t.r]}

    # An undeclared subquery takes its root's declared schema (R59).
    inherits =
      q(ctx, alt(p(ctx), ctx))
      |> Selecto.filter({"id", {:subquery, :in, rated(ctx)}})
      |> Selecto.select(["name"])

    assert outcome(inherits) == [["alt-b"]]

    # A declared non-association join under a root declaring alt reads alt.
    domain =
      put_in(alt(p(ctx), ctx), [:joins], %{
        lkd: %{
          non_assoc: true,
          source: ctx.t.lk,
          name: "lkd",
          type: :left,
          owner_key: :id,
          related_key: :product_id,
          fields: %{"label" => %{type: :string}}
        }
      })

    assert outcome(q(ctx, domain) |> Selecto.select(["name", "lkd.label"])) ==
             [["alt-a", "ALT-LK"], ["alt-b", nil]]
  end

  # A CTE named like the table p, its body over r with p's columns.
  defp fake_p(query, ctx) do
    Selecto.with_cte(
      query,
      ctx.t.p,
      fn -> q(ctx, r(ctx)) |> Selecto.select(["product_id", "title", "tenant_id"]) end,
      columns: ["id", "name", "tenant_id"]
    )
  end

  defp reviews_with_product(ctx, extra \\ %{}) do
    product = Map.merge(p(ctx).source, extra)

    r(ctx)
    |> put_in([:source, :associations], %{
      prod: %{queryable: :prod, field: :prod, owner_key: :product_id, related_key: :id}
    })
    |> Map.put(:schemas, %{prod: product})
    |> Map.put(:joins, %{prod: %{type: :left, name: "prod"}})
  end

  test "only an undeclared root reads a list of its name; joined and declared tables stay tables",
       ctx do
    # A joined schema over table p, and a CTE named p: refused.
    joined =
      q(ctx, reviews_with_product(ctx)) |> fake_p(ctx) |> Selecto.select(["title", "prod.name"])

    assert refusal(joined) == {:cte_name_collision, [ctx.t.p]}

    # A root declaring its schema is a table: refused.
    declared =
      q(ctx, put_in(p(ctx), [:source, :source_schema], "public"))
      |> fake_p(ctx)
      |> Selecto.select(["name"])

    assert refusal(declared) == {:cte_name_collision, [ctx.t.p]}

    # A body rooted on a declared table named like an earlier CTE: refused.
    body = dom("cte_cb", id: :integer, name: :string)

    nested =
      q(ctx, body)
      |> fake_p(ctx)
      |> Selecto.with_cte(
        "cte_cb",
        fn ->
          q(ctx, put_in(p(ctx), [:source, :source_schema], "public"))
          |> Selecto.select(["id", "name"])
        end,
        columns: ["id", "name"],
        dependencies: [ctx.t.p]
      )
      |> Selecto.select(["name"])

    assert refusal(nested) == {:cte_name_collision, [ctx.t.p]}

    # An undeclared root named like its own CTE.
    undeclared = q(ctx, p(ctx)) |> fake_p(ctx) |> Selecto.select(["name"])
    # On PostgreSQL the table p is on the search_path: which one was meant
    # cannot be told, so it refuses.
    assert refusal(undeclared) == {:cte_name_collision, [ctx.t.p]}
  end

  defp tier(query, condition),
    do: Selecto.case_when_select(query, [{[condition], "HIGH"}], else: "LOW", as: "tier")

  test "a CASE subquery reads its table; a list named like it refuses", ctx do
    exists =
      q(ctx, r(ctx))
      |> Selecto.select(["id"])
      |> Selecto.filter({"product_id", 2})
      |> Selecto.filter({"rating", {:>=, 4}})

    for condition <- [
          {"id", {:subquery, :in, rated(ctx)}},
          {"id", "=", {:subquery, :any, rated(ctx)}},
          {:exists, exists}
        ] do
      query = q(ctx, p(ctx)) |> values_r(ctx) |> Selecto.select(["name"]) |> tier(condition)
      assert refusal(query) == {:cte_name_collision, [ctx.t.r]}
    end

    control =
      q(ctx, p(ctx)) |> Selecto.select(["name"]) |> tier({"id", {:subquery, :in, rated(ctx)}})

    assert outcome(control) == [["a", "HIGH"], ["b", "LOW"]]
  end

  test "raw CTEs: unreferenced, joined, and named like a table read", ctx do
    plain = q(ctx, p(ctx)) |> Selecto.with_ctes([{:raw_cte, "raw_one AS (SELECT 1 AS v)", []}])
    assert outcome(Selecto.select(plain, ["name"])) == [["a"], ["b"]]

    joined =
      q(ctx, p(ctx))
      |> Selecto.with_ctes([{:raw_cte, "raw_two (rid, v) AS (SELECT 1, 7)", []}])
      |> Selecto.join(:rw,
        source: "raw_two",
        source_kind: :cte,
        type: :left,
        owner_key: :id,
        related_key: :rid,
        fields: %{"v" => %{type: :integer}}
      )
      |> Selecto.select(["name", "rw.v"])

    assert outcome(joined) == [["a", 7], ["b", nil]]

    # A raw CTE named like a table a filter subquery reads refuses.
    named =
      q(ctx, p(ctx))
      |> Selecto.with_ctes([{:raw_cte, "#{ctx.t.r} AS (SELECT 2 AS product_id)", []}])
      |> Selecto.filter({"id", {:subquery, :in, rated(ctx)}})
      |> Selecto.select(["name"])

    assert refusal(named) == {:cte_name_collision, [ctx.t.r]}
  end

  test "an embedded query missing its required tenant refuses typed", ctx do
    inner =
      q(ctx, Map.put(dom(ctx.t.r, @rf, %{tenant_field: :tenant_id}), :tenant_required, true))
      |> Selecto.select(["product_id"])

    query =
      q(ctx, p(ctx))
      |> Selecto.filter({"id", {:subquery, :in, inner}})
      |> Selecto.select(["name"])

    assert {:error, %Selecto.Error{details: %{code: :tenant_scope_missing}}} =
             Selecto.execute(query)
  end

  test "a CASE subquery resolves under its own declaration, or the root's", ctx do
    inner = q(ctx, alt(r(ctx), ctx)) |> Selecto.select(["product_id"])

    both =
      q(ctx, r(ctx)) |> Selecto.select(["title"]) |> tier({"product_id", {:subquery, :in, inner}})

    assert refusal(both) == {:relation_ambiguous, [ctx.t.r]}

    inherits =
      q(ctx, alt(p(ctx), ctx))
      |> Selecto.select(["name"])
      |> tier({"id", {:subquery, :in, rated(ctx)}})

    assert outcome(inherits) == [["alt-a", "LOW"], ["alt-b", "HIGH"]]
  end

  defp raw_r(ctx),
    do: "SELECT 2 AS id, 2 AS product_id, 5 AS rating, 'RAW' AS title, 1 AS tenant_id"

  defp raw_p(_ctx), do: "SELECT 1 AS id, 'RAWP' AS name, 1 AS tenant_id"

  defp raw_texts(name, body) do
    [
      "/* c */ #{name} AS (#{body})",
      "-- c\n #{name} AS (#{body})",
      "x13 AS (SELECT 1 AS v), #{name} AS (#{body})",
      "`#{name}` AS (#{body})"
    ]
  end

  test "every name a raw CTE's text defines counts; unreadable text refuses", ctx do
    for text <- raw_texts(ctx.t.r, raw_r(ctx)) do
      query =
        q(ctx, p(ctx))
        |> Selecto.with_ctes([{:raw_cte, text, []}])
        |> Selecto.filter({"id", {:subquery, :in, rated(ctx)}})
        |> Selecto.select(["name"])

      assert refusal(query) == {:cte_name_collision, [ctx.t.r]}, text
    end

    for text <- raw_texts(ctx.t.p, raw_p(ctx)) do
      query =
        q(ctx, reviews_with_product(ctx))
        |> Selecto.with_ctes([{:raw_cte, text, []}])
        |> Selecto.select(["title", "prod.name"])

      assert refusal(query) == {:cte_name_collision, [ctx.t.p]}, text
    end

    two = "x13 AS (SELECT 1 AS v), #{ctx.t.r} AS (#{raw_r(ctx)})"

    rooted =
      q(ctx, r(ctx)) |> Selecto.with_ctes([{:raw_cte, two, []}]) |> Selecto.select(["title"])

    # On PostgreSQL the table r is on the search_path: refused.
    assert refusal(rooted) == {:cte_name_collision, [ctx.t.r]}

    unreadable = q(ctx, p(ctx)) |> Selecto.with_ctes([{:raw_cte, "SELECT 1", []}])

    assert {:error, %Selecto.Error{details: %{reason: :raw_cte_unparsed}}} =
             Selecto.execute(Selecto.select(unreadable, ["name"]))
  end

  test "a query member the read does not use is not checked", ctx do
    member =
      q(ctx, Map.put(dom(ctx.t.r, @rf, %{tenant_field: :tenant_id}), :tenant_required, true))
      |> Selecto.select(["product_id"])

    domain = Map.put(p(ctx), :query_members, %{unused: %{query: member}})
    assert outcome(q(ctx, domain) |> Selecto.select(["name"])) == [["a"], ["b"]]
  end

  test "a mixed-case list root reads its list; a name an identifier cannot hold refuses", ctx do
    mixed =
      q(ctx, dom("MixedList", id: :integer, name: :string))
      |> Selecto.with_values([[1, "M"]], columns: ["id", "name"], as: "MixedList")
      |> Selecto.select(["name"])

    assert outcome(mixed) == [["M"]]

    cte =
      q(ctx, dom("MixedCte", id: :integer, name: :string))
      |> Selecto.with_cte("MixedCte", fn -> q(ctx, p(ctx)) |> Selecto.select(["id", "name"]) end,
        columns: ["id", "name"]
      )
      |> Selecto.select(["name"])

    assert outcome(cte) == [["a"], ["b"]]

    bad = ~S[bad"name]

    quoted =
      q(ctx, dom(bad, id: :integer, name: :string))
      |> Selecto.with_values([[1, "M"]], columns: ["id", "name"], as: bad)
      |> Selecto.select(["name"])

    assert refusal(quoted) == {:invalid_identifier, [bad]}
  end
end
