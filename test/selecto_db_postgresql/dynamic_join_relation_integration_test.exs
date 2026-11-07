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

  test "a subquery whose domain declares its schema reads there", ctx do
    assert rows(Selecto.execute(subquery_join(ctx, ctx.alt))) == [["a", "alt"], ["b", nil]]
  end
end
