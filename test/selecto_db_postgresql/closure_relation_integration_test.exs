defmodule SelectoDBPostgreSQL.ClosureRelationIntegrationTest do
  # A closure-table join reads its closure table like any other relation of the
  # read: its own generated CTE never hides it, even when the join's id makes
  # the CTE's old name equal the table's, and another schema declared in the
  # statement never decides which closure table it reads.
  use ExUnit.Case, async: false

  alias SelectoDBPostgreSQL.Adapter
  alias SelectoDBPostgreSQL.Verification.ConnectionOptions

  @moduletag :postgres

  setup do
    opts = Keyword.put(ConnectionOptions.options(), :pool_size, 1)
    {:ok, c} = Adapter.connect(opts)
    Process.unlink(c)
    n = System.unique_integer([:positive])

    # The alternate schema's name sorts before "public", so a bare closure-table
    # name would reach it first on the statement's pinned search_path.
    t = %{
      alt: "cl_alt_#{n}",
      cat: "cl_cat_#{n}",
      ci: "cl_ci_#{n}",
      p: "cl_p_#{n}"
    }

    for sql <- [
          "CREATE SCHEMA #{t.alt}",
          "CREATE TABLE public.#{t.cat} (id int PRIMARY KEY, name text)",
          "CREATE TABLE #{t.alt}.#{t.cat} (id int PRIMARY KEY, name text)",
          "CREATE TABLE public.#{t.cat}_closure (ancestor_id int, descendant_id int, depth int)",
          "CREATE TABLE #{t.alt}.#{t.cat}_closure (ancestor_id int, descendant_id int, depth int)",
          "CREATE TABLE public.#{t.ci} (id int PRIMARY KEY, cat_id int)",
          "CREATE TABLE #{t.alt}.#{t.p} (id int PRIMARY KEY, name text)",
          "INSERT INTO public.#{t.cat} VALUES (1, 'pub-root'), (2, 'pub-kid')",
          "INSERT INTO #{t.alt}.#{t.cat} VALUES (1, 'alt-root'), (2, 'alt-kid')",
          "INSERT INTO public.#{t.cat}_closure VALUES (1, 1, 0), (1, 2, 1), (2, 2, 0)",
          "INSERT INTO #{t.alt}.#{t.cat}_closure VALUES (1, 1, 10), (1, 2, 11), (2, 2, 10)",
          "INSERT INTO public.#{t.ci} VALUES (1, 2)",
          "CREATE TABLE public.#{t.cat}_cc2 (ancestor_id int, descendant_id int, depth int)",
          "INSERT INTO public.#{t.cat}_cc2 VALUES (1, 1, 20), (1, 2, 21), (2, 2, 20)",
          "INSERT INTO #{t.alt}.#{t.p} VALUES (1, 'alt-a')"
        ],
        do: {:ok, _} = Adapter.execute(c, sql, [], [])

    on_exit(fn ->
      {:ok, d} = Adapter.connect(opts)
      Adapter.execute(d, "DROP SCHEMA IF EXISTS #{t.alt} CASCADE", [], [])

      Adapter.execute(
        d,
        "DROP TABLE IF EXISTS public.#{t.cat}, public.#{t.cat}_closure, public.#{t.ci}, " <>
          "public.#{t.cat}_cc2",
        [],
        []
      )

      GenServer.stop(d)
    end)

    %{connection: c, t: t}
  end

  # Items joined to a closure-table hierarchy over categories. The join's id is
  # the category table's name, so the default closure table is "<id>_closure".
  defp domain(ctx, with_alt_association?) do
    cat = String.to_atom(ctx.t.cat)

    base = %{
      name: ctx.t.ci,
      source: %{
        source_table: ctx.t.ci,
        primary_key: :id,
        fields: [:id, :cat_id],
        redact_fields: [],
        columns: %{id: %{type: :integer}, cat_id: %{type: :integer}},
        associations: %{
          cat => %{queryable: :catq, field: cat, owner_key: :cat_id, related_key: :id}
        }
      },
      schemas: %{
        catq: %{
          source_table: ctx.t.cat,
          primary_key: :id,
          fields: [:id, :name],
          redact_fields: [],
          columns: %{id: %{type: :integer}, name: %{type: :string}},
          associations: %{}
        }
      },
      joins: %{cat => %{type: :hierarchical, name: "cat", hierarchy_type: :closure_table}}
    }

    if with_alt_association? do
      prod = %{
        source_table: ctx.t.p,
        source_schema: ctx.t.alt,
        primary_key: :id,
        fields: [:id, :name],
        redact_fields: [],
        columns: %{id: %{type: :integer}, name: %{type: :string}},
        associations: %{}
      }

      base
      |> put_in([:source, :associations, :prod], %{
        queryable: :prod,
        field: :prod,
        owner_key: :id,
        related_key: :id,
        cardinality: :one
      })
      |> put_in([:schemas, :prod], prod)
      |> put_in([:joins, :prod], %{type: :left, name: "prod"})
    else
      base
    end
  end

  defp depths(query) do
    {:ok, {rows, _columns, _aliases}} = Selecto.execute(query)
    rows |> Enum.map(&List.last/1) |> Enum.sort()
  end

  test "a closure join named like its table reads the public closure table", ctx do
    cat = ctx.t.cat

    plain =
      Selecto.configure(domain(ctx, false), ctx.connection, adapter: Adapter, validate: false)
      |> Selecto.select(["id", "#{cat}_depth"])

    {sql, _params} = Selecto.to_sql(plain)
    assert sql =~ "__selecto_closure_#{cat}"
    assert depths(plain) == [0, 1]
  end

  test "another schema declared in the statement does not move the closure table", ctx do
    cat = ctx.t.cat

    declared =
      Selecto.configure(domain(ctx, true), ctx.connection, adapter: Adapter, validate: false)
      |> Selecto.select(["id", "#{cat}_depth"])

    assert depths(declared) == [0, 1]

    inner =
      Selecto.configure(
        %{
          name: ctx.t.p,
          source: %{
            source_table: ctx.t.p,
            source_schema: ctx.t.alt,
            primary_key: :id,
            fields: [:id, :name],
            redact_fields: [],
            columns: %{id: %{type: :integer}, name: %{type: :string}},
            associations: %{}
          },
          schemas: %{},
          joins: %{}
        },
        ctx.connection,
        adapter: Adapter,
        validate: false
      )
      |> Selecto.select(["id", "name"])

    subquery =
      Selecto.configure(domain(ctx, false), ctx.connection, adapter: Adapter, validate: false)
      |> Selecto.DynamicJoin.join_subquery(:sq, inner, on: [%{left: "id", right: "id"}])
      |> Selecto.select(["id", "sq.name", "#{cat}_depth"])

    assert depths(subquery) == [0, 1]
  end

  test "a CTE body's closure join with the root's id reads its own closure table", ctx do
    cat = ctx.t.cat

    root =
      Selecto.configure(domain(ctx, false), ctx.connection, adapter: Adapter, validate: false)

    body = fn ->
      ctx
      |> domain(false)
      |> put_in([:joins, String.to_atom(cat), :closure_table], "#{cat}_cc2")
      |> Selecto.configure(ctx.connection, adapter: Adapter, validate: false)
      |> Selecto.select(["id", "#{cat}_depth"])
    end

    query =
      root
      |> Selecto.with_cte("cb", body,
        columns: ["rid", "cdepth"],
        join: [owner_key: :id, related_key: :rid, fields: %{"cdepth" => %{type: :integer}}]
      )
      |> Selecto.select(["id", "#{cat}_depth", "cb.cdepth"])

    {:ok, {rows, _, _}} = Selecto.execute(query)
    assert Enum.sort(rows) == [[1, 0, 20], [1, 0, 21], [1, 1, 20], [1, 1, 21]]
  end

  test "a list named like the closure table a join reads refuses", ctx do
    cat = ctx.t.cat

    query =
      Selecto.configure(domain(ctx, false), ctx.connection, adapter: Adapter, validate: false)
      |> Selecto.with_values([[1, 1, 10]],
        columns: ["ancestor_id", "descendant_id", "depth"],
        as: "#{cat}_closure"
      )
      |> Selecto.select(["id", "#{cat}_depth"])

    assert {:error, %Selecto.Error{details: %{reason: :cte_name_collision, relations: [name]}}} =
             Selecto.execute(query)

    assert name == "#{cat}_closure"
  end
end
