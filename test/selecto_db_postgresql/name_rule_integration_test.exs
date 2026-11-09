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
      if Process.alive?(c), do: GenServer.stop(c)
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

  test "a raw CTE's iodata text with parameters is read and runs", ctx do
    raw = {:raw_cte, ["x14 AS (SELECT ", {:param, 1}, "::int AS pid)"], []}

    query =
      q(ctx, p(ctx))
      |> Selecto.with_ctes([raw])
      |> Selecto.join(:xj,
        source: "x14",
        source_kind: :cte,
        type: :left,
        owner_key: :id,
        related_key: :pid,
        fields: %{"pid" => %{type: :integer}}
      )
      |> Selecto.select(["name", "xj.pid"])

    assert outcome(query) == [["a", 1], ["b", nil]]
  end

  test "a mixed-case list named unquoted in host SQL reads the list", ctx do
    list = String.capitalize(ctx.t.lk)

    query =
      q(ctx, p(ctx))
      |> Selecto.with_values([[1]], columns: ["pid"], as: list)
      |> Selecto.filter({:raw_sql_filter, ["selecto_root.id IN (SELECT pid FROM #{list})"]})
      |> Selecto.select(["name"])

    assert outcome(query) == [["a"]]

    # A raw CTE's unquoted mixed-case name and an ad-hoc join spelled the same
    # read one list.
    raw = {:raw_cte, "MixR AS (SELECT 1 AS id, 'RAWR' AS v)", []}

    joined =
      q(ctx, p(ctx))
      |> Selecto.with_ctes([raw])
      |> Selecto.join(:mj,
        source: "MixR",
        type: :left,
        owner_key: :id,
        related_key: :id,
        fields: %{"v" => %{type: :string}}
      )
      |> Selecto.select(["name", "mj.v"])

    assert outcome(joined) == [["a", "RAWR"], ["b", nil]]
  end

  test "a $ inside a raw CTE's identifier does not hide the next definition", ctx do
    text = "x AS (SELECT 1 AS a$q$), #{ctx.t.r} AS (#{raw_r(ctx)})"

    query =
      q(ctx, p(ctx))
      |> Selecto.with_ctes([{:raw_cte, text, []}])
      |> Selecto.filter({"id", {:subquery, :in, rated(ctx)}})
      |> Selecto.select(["name"])

    assert refusal(query) == {:cte_name_collision, [ctx.t.r]}
  end

  test "an ad-hoc join on a list whose name an identifier cannot hold refuses typed", ctx do
    query =
      q(ctx, p(ctx))
      |> Selecto.with_values([[1, "V"]], columns: ["id", "v"], as: "v$x")
      |> Selecto.join(:vj,
        source: "v$x",
        type: :left,
        owner_key: :id,
        related_key: :id,
        fields: %{"v" => %{type: :string}}
      )
      |> Selecto.select(["name", "vj.v"])

    assert refusal(query) == {:invalid_identifier, ["v$x"]}
  end

  test "a list named with a word any supported dialect reserves reads its rows", ctx do
    for name <- ~w(With Desc Window To Lateral Rank Rows Groups) do
      joined =
        q(ctx, p(ctx))
        |> Selecto.with_values([[1, "V"]], columns: ["id", "v"], as: name)
        |> Selecto.join(:vj,
          source: name,
          type: :left,
          owner_key: :id,
          related_key: :id,
          fields: %{"v" => %{type: :string}}
        )
        |> Selecto.select(["name", "vj.v"])

      assert outcome(joined) == [["a", "V"], ["b", nil]], name

      rooted =
        q(ctx, dom(name, id: :integer, v: :string))
        |> Selecto.with_values([[1, "V"]], columns: ["id", "v"], as: name)
        |> Selecto.select(["v"])

      assert outcome(rooted) == [["V"]], name

      cte =
        q(ctx, p(ctx))
        |> Selecto.with_cte(name, fn -> q(ctx, p(ctx)) |> Selecto.select(["id", "name"]) end,
          columns: ["id", "v"]
        )
        |> Selecto.join(:cj,
          source: name,
          type: :left,
          owner_key: :id,
          related_key: :id,
          fields: %{"v" => %{type: :string}}
        )
        |> Selecto.select(["name", "cj.v"])

      assert outcome(cte) == [["a", "a"], ["b", "b"]], name
    end
  end

  test "a non-ASCII identifier before $$ does not hide the next definition", ctx do
    text = "x AS (SELECT 1 AS ä$$), #{ctx.t.r} AS (#{raw_r(ctx)}), y AS (SELECT 2 AS ö$$)"

    query =
      q(ctx, p(ctx))
      |> Selecto.with_ctes([{:raw_cte, text, []}])
      |> Selecto.filter({"id", {:subquery, :in, rated(ctx)}})
      |> Selecto.select(["name"])

    assert refusal(query) == {:cte_name_collision, [ctx.t.r]}
  end

  test "a carriage return ends a -- comment in a raw CTE's text", ctx do
    for text <- [
          "x AS (SELECT 1 AS v) -- c\r, #{ctx.t.r} AS (#{raw_r(ctx)}) -- e\n",
          "x AS (SELECT 1 AS v -- c\r), #{ctx.t.r} AS (#{raw_r(ctx)} -- e\n)"
        ] do
      query =
        q(ctx, p(ctx))
        |> Selecto.with_ctes([{:raw_cte, text, []}])
        |> Selecto.filter({"id", {:subquery, :in, rated(ctx)}})
        |> Selecto.select(["name"])

      assert refusal(query) == {:cte_name_collision, [ctx.t.r]}, inspect(text)
    end
  end

  test "a list root a quoted relation of its exact name answers to refuses", ctx do
    table = "NrK#{System.unique_integer([:positive])}"

    {:ok, _} =
      Adapter.execute(
        ctx.conn,
        ~s|CREATE TABLE public."#{table}" (id INT PRIMARY KEY, name TEXT, tenant_id INT)|,
        [],
        []
      )

    try do
      {:ok, _} =
        Adapter.execute(
          ctx.conn,
          ~s|INSERT INTO public."#{table}" VALUES (1, 'TBL-K', 1)|,
          [],
          []
        )

      assert outcome(q(ctx, dom(table, @pf)) |> Selecto.select(["name"])) == [["TBL-K"]]

      listed =
        q(ctx, dom(table, @pf))
        |> Selecto.with_values([[5, "V-K", 1]], columns: ["id", "name", "tenant_id"], as: table)
        |> Selecto.select(["name"])

      assert refusal(listed) == {:cte_name_collision, [table]}
    after
      Adapter.execute(ctx.conn, ~s|DROP TABLE IF EXISTS public."#{table}"|, [], [])
    end
  end

  test "an unused declared join reads nothing: a missing table or a list named like it", ctx do
    join = fn source ->
      %{
        non_assoc: true,
        source: source,
        type: :left,
        owner_key: :id,
        related_key: :product_id,
        fields: %{"label" => %{type: :string}}
      }
    end

    domain =
      put_in(p(ctx), [:joins], %{
        gone: Map.put(join.("nr_gone_#{System.unique_integer([:positive])}"), :name, "gone"),
        lkd: Map.put(join.(ctx.t.lk), :name, "lkd")
      })

    query = q(ctx, domain)
    assert outcome(Selecto.select(query, ["name"])) == [["a"], ["b"]]
    assert outcome(Selecto.select(query, ["name", "lkd.label"])) == [["a", "TBL-LK"], ["b", nil]]

    named =
      Selecto.with_values(query, [[1, "V"]], columns: ["product_id", "label"], as: ctx.t.lk)

    assert outcome(Selecto.select(named, ["name"])) == [["a"], ["b"]]

    assert refusal(Selecto.select(named, ["name", "lkd.label"])) ==
             {:cte_name_collision, [ctx.t.lk]}

    assert refusal(named |> Selecto.select(["name"]) |> Selecto.filter({"lkd.label", "x"})) ==
             {:cte_name_collision, [ctx.t.lk]}
  end

  test "a list named with a word only another dialect reserves reads as host text names it",
       ctx do
    listed =
      q(ctx, p(ctx))
      |> Selecto.with_values([[1]], columns: ["pid"], as: "Plan")
      |> Selecto.filter({:raw_sql_filter, ["selecto_root.id IN (SELECT pid FROM Plan)"]})
      |> Selecto.select(["name"])

    assert outcome(listed) == [["a"]]

    raw =
      q(ctx, p(ctx))
      |> Selecto.with_ctes([{:raw_cte, "Plan AS (SELECT 1 AS id, 'RAW' AS v)", []}])
      |> Selecto.join(:mj,
        source: "Plan",
        type: :left,
        owner_key: :id,
        related_key: :id,
        fields: %{"v" => %{type: :string}}
      )
      |> Selecto.select(["name", "mj.v"])

    assert outcome(raw) == [["a", "RAW"], ["b", nil]]
  end

  test "a raw CTE with params inside a subquery join binds them", ctx do
    inner =
      q(ctx, dom("pa", pid: :integer, label: :string))
      |> Selecto.with_ctes([
        {:raw_cte, "pa AS (SELECT CAST($1 AS INTEGER) AS pid, 'P' AS label)", [1]}
      ])
      |> Selecto.select(["pid", "label"])

    query =
      q(ctx, p(ctx))
      |> Selecto.DynamicJoin.join_subquery(:sq, inner, on: [%{left: "id", right: "pid"}])
      |> Selecto.select(["name", "sq.label"])

    assert outcome(query) == [["a", "P"], ["b", nil]]
  end

  test "a raw CTE placeholder its params do not bind refuses typed", ctx do
    one = "CAST($1 AS INTEGER)"
    two = String.replace("CAST($1 AS INTEGER)", "$1", "$2")

    for {text, params} <- [
          {"pa AS (SELECT #{one} AS pid, #{two} AS label)", [5]},
          {"pa AS (SELECT 7 AS pid, #{one} AS label)", []},
          {["pa AS (SELECT ", {:param, 5}, " AS pid, 'P' AS label)"], [6]}
        ] do
      query =
        q(ctx, dom("pa", pid: :integer, label: :string))
        |> Selecto.with_ctes([{:raw_cte, text, params}])
        |> Selecto.select(["pid", "label"])
        |> Selecto.filter({"label", "OUTER"})

      assert {:error, %Selecto.Error{details: %{reason: :raw_cte_params_mismatch}}} =
               Selecto.execute(query),
             inspect(text)
    end
  end

  test "on PostgreSQL a reserved-only list name reads as host text folds it", ctx do
    {:ok, _} =
      Adapter.execute(ctx.conn, ~s|CREATE TABLE public."Rank" (id INT, v TEXT)|, [], [])

    try do
      {:ok, _} =
        Adapter.execute(ctx.conn, ~s|INSERT INTO public."Rank" VALUES (1, 'TBL')|, [], [])

      raw =
        q(ctx, p(ctx))
        |> Selecto.with_ctes([{:raw_cte, "Rank AS (SELECT 1 AS id, 'RAW' AS v)", []}])
        |> Selecto.join(:mj,
          source: "Rank",
          type: :left,
          owner_key: :id,
          related_key: :id,
          fields: %{"v" => %{type: :string}}
        )
        |> Selecto.select(["name", "mj.v"])

      assert outcome(raw) == [["a", "RAW"], ["b", nil]]

      for name <- ~w(First Query Action Position RANK) do
        joined =
          q(ctx, p(ctx))
          |> Selecto.with_ctes([{:raw_cte, "#{name} AS (SELECT 1 AS id, 'RAW' AS v)", []}])
          |> Selecto.join(:mj,
            source: name,
            type: :left,
            owner_key: :id,
            related_key: :id,
            fields: %{"v" => %{type: :string}}
          )
          |> Selecto.select(["name", "mj.v"])

        assert outcome(joined) == [["a", "RAW"], ["b", nil]], name
      end
    after
      Adapter.execute(ctx.conn, ~s|DROP TABLE IF EXISTS public."Rank"|, [], [])
    end

    {:ok, _} = Adapter.execute(ctx.conn, "CREATE TABLE public.rank (pid INT)", [], [])

    try do
      {:ok, _} = Adapter.execute(ctx.conn, "INSERT INTO public.rank VALUES (2)", [], [])

      listed =
        q(ctx, p(ctx))
        |> Selecto.with_values([[1]], columns: ["pid"], as: "Rank")
        |> Selecto.filter({:raw_sql_filter, ["selecto_root.id IN (SELECT pid FROM Rank)"]})
        |> Selecto.select(["name"])

      assert outcome(listed) == [["a"]]
    after
      Adapter.execute(ctx.conn, "DROP TABLE IF EXISTS public.rank", [], [])
    end
  end

  test "placeholder-like text in a raw CTE's literals is text without params, nested too", ctx do
    lit = "cost $1"

    inner =
      q(ctx, dom("pa", pid: :integer, v: :string))
      |> Selecto.with_ctes([
        {:raw_cte, "pa AS (SELECT 1 AS pid, '#{lit}' AS v /* #{lit} */ -- #{lit}\n)", []}
      ])
      |> Selecto.select(["pid", "v"])
      |> Selecto.filter({"pid", 1})

    assert outcome(inner) == [[1, lit]]

    nested =
      q(ctx, p(ctx))
      |> Selecto.DynamicJoin.join_subquery(:sq, inner, on: [%{left: "id", right: "pid"}])
      |> Selecto.select(["name", "sq.v"])
      |> Selecto.filter({"name", "a"})

    assert outcome(nested) == [["a", lit]]

    # A nested raw filter with the same text beside a bound param.
    sub =
      q(ctx, p(ctx))
      |> Selecto.filter({:raw_sql_filter, ["selecto_root.name <> '#{lit}'"]})
      |> Selecto.filter({"id", 1})
      |> Selecto.select(["id"])

    filtered =
      q(ctx, p(ctx))
      |> Selecto.filter({"id", {:subquery, :in, sub}})
      |> Selecto.select(["name"])

    assert outcome(filtered) == [["a"]]
  end

  test "a raw CTE whose params leave a $n unbound refuses typed", ctx do
    query =
      q(ctx, dom("pa", pid: :integer))
      |> Selecto.with_ctes([{:raw_cte, "pa AS (SELECT CAST($2 AS INTEGER) AS pid)", [5, 6]}])
      |> Selecto.select(["pid"])

    assert {:error, %Selecto.Error{details: %{reason: :raw_cte_params_mismatch}}} =
             Selecto.execute(query)
  end

  test "on PostgreSQL a list name is spelled as Selecto always spelled it", ctx do
    read = fn name, from ->
      q(ctx, p(ctx))
      |> Selecto.with_values([[1]], columns: ["pid"], as: name)
      |> Selecto.filter({:raw_sql_filter, ["selecto_root.id IN (SELECT pid FROM #{from})"]})
      |> Selecto.select(["name"])
      |> outcome()
    end

    # Selecto always quoted Order and Key: host text quoting them reads the list.
    assert read.("Order", ~s("Order")) == [["a"]]
    assert read.("Key", ~s("Key")) == [["a"]]
    refute read.("Key", "Key") == [["a"]]

    # Rank it left unquoted: host text naming it unquoted reads the list.
    assert read.("Rank", "Rank") == [["a"]]
    refute read.("Rank", ~s("Rank")) == [["a"]]
  end
end
