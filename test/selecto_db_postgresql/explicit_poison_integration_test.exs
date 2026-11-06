defmodule SelectoDBPostgreSQL.ExplicitPoisonIntegrationTest do
  # R46 FINAL-2 / R47 / R48 / R49 / R50 live: a role with CREATE on a pinned
  # schema (public here) plants exact-match operators and functions for every
  # column type the corpus compares. Each entry of the corpus, the specialised
  # builders included, runs on a clean database and again under the plants,
  # and must answer the same: zero hijacks. citext lives in its own schema
  # and keeps its semantics; a user-defined aggregate or operator planted
  # beside its members over a domain is never chosen (R50).
  use ExUnit.Case, async: false

  alias Selecto.Write.Command
  alias SelectoDBPostgreSQL.{Adapter, ExplicitSQL}

  @moduletag :postgres

  defp options, do: SelectoDBPostgreSQL.Verification.ConnectionOptions.options()

  defp exec!(c, sql) do
    case Adapter.execute(c, sql, [], []) do
      {:ok, result} -> result
      other -> flunk("#{sql}: #{inspect(other)}")
    end
  end

  setup_all do
    prefix = System.get_env("SELECTO_TEST_DATABASE_PREFIX", "selecto")
    database = "#{prefix}_poison_#{System.unique_integer([:positive])}"
    {:ok, admin} = Adapter.connect(Keyword.put(options(), :pool_size, 1))
    Process.unlink(admin)
    exec!(admin, ~s|CREATE DATABASE "#{database}"|)
    exec!(admin, ~s|ALTER DATABASE "#{database}" SET search_path = public, ext|)

    opts = options() |> Keyword.put(:pool_size, 1) |> Keyword.put(:database, database)
    {:ok, setup} = Adapter.connect(opts)
    Process.unlink(setup)

    for sql <- [
          "CREATE SCHEMA ext",
          "CREATE EXTENSION citext SCHEMA ext",
          "CREATE EXTENSION hstore SCHEMA ext",
          "CREATE EXTENSION pgcrypto SCHEMA ext",
          "CREATE TYPE en AS ENUM ('a', 'b')",
          "CREATE DOMAIN dom AS varchar(10)",
          "CREATE DOMAIN cidom AS ext.citext",
          "CREATE TABLE items (id int PRIMARY KEY, org varchar(10) NOT NULL, name text, vc varchar(20), " <>
            "kind en, tags int[], code dom, ci ext.citext, dci cidom, n int, parent_id int, cat_id int, " <>
            "path text, ts timestamptz)",
          "INSERT INTO items VALUES " <>
            "(1, 't1', 'alpha', 'alpha', 'a', '{1,2}', 'c1', 'MiXed', 'Bob', 5, NULL, 10, '1', '2024-01-01'), " <>
            "(2, 't2', 'SECRET', 'SECRET', 'b', '{3}', 'c2', 'other', 'eve', 7, NULL, 20, '2', '2024-01-02'), " <>
            "(3, 't1', 'mixed', 'mixed', 'b', '{2}', 'c3', 'mixed', 'BOB', 9, 1, 10, '1/3', '2024-01-03'), " <>
            "(5, 't1', 'MIXED', 'MIXED', 'a', '{9}', 'c5', 'mixed', 'al', 11, 3, 30, '1/3/5', '2024-01-05')",
          "CREATE TABLE cats (id int PRIMARY KEY, org varchar(10) NOT NULL, name text)",
          "INSERT INTO cats VALUES (10, 't1', 'ten'), (20, 't2', 'twenty'), (30, 't1', 'thirty')",
          "CREATE TABLE labels (id int PRIMARY KEY, org varchar(10) NOT NULL, name text)",
          "INSERT INTO labels VALUES (100, 't1', 'red'), (200, 't2', 'blue')",
          "CREATE TABLE label_labels (item_id int, label_id int)",
          "INSERT INTO label_labels VALUES (1, 100), (2, 200), (3, 100)",
          "CREATE TABLE codes (code ext.citext PRIMARY KEY, org varchar(10) NOT NULL, label text)",
          "INSERT INTO codes VALUES ('MIXED', 't1', 'tag-mixed'), ('other', 't2', 'tag-other')"
        ],
        do: exec!(setup, sql)

    GenServer.stop(setup)

    on_exit(fn ->
      {:ok, admin} = Adapter.connect(Keyword.put(options(), :pool_size, 1))
      exec!(admin, ~s|DROP DATABASE IF EXISTS "#{database}" WITH (FORCE)|)
      GenServer.stop(admin)
    end)

    %{opts: opts}
  end

  @plants [
    {"varchar", "varchar"},
    {"text", "text"},
    {"en", "en"},
    {"int4[]", "int4[]"},
    {"dom", "dom"},
    {"cidom", "cidom"},
    {"ext.citext", "pg_catalog.text"},
    {"pg_catalog.text", "ext.citext"},
    {"ext.citext", "varchar"},
    {"varchar", "ext.citext"}
  ]

  defp plants do
    operators =
      for {{left, right}, index} <- Enum.with_index(@plants),
          op <- ~w(= <> < <= > >= ~~ ~~* !~~) do
        [
          "CREATE FUNCTION public.poison_#{index}_#{:erlang.phash2(op)}(#{left}, #{right}) RETURNS boolean " <>
            "LANGUAGE sql IMMUTABLE AS 'SELECT true'",
          "CREATE OPERATOR public.#{op} (LEFTARG = #{left}, RIGHTARG = #{right}, " <>
            "FUNCTION = public.poison_#{index}_#{:erlang.phash2(op)})"
        ]
      end

    List.flatten(operators) ++
      [
        "CREATE FUNCTION public.poison_cat(varchar, varchar) RETURNS varchar LANGUAGE sql IMMUTABLE AS $$SELECT 'poison'::varchar$$",
        "CREATE OPERATOR public.|| (LEFTARG = varchar, RIGHTARG = varchar, FUNCTION = public.poison_cat)",
        "CREATE FUNCTION public.poison_and(smallint, integer) RETURNS integer LANGUAGE sql IMMUTABLE AS 'SELECT 0'",
        "CREATE OPERATOR public.& (LEFTARG = smallint, RIGHTARG = integer, FUNCTION = public.poison_and)",
        "CREATE FUNCTION public.lower(varchar) RETURNS text LANGUAGE sql IMMUTABLE AS $$SELECT 'alpha'::text$$",
        "CREATE FUNCTION public.upper(varchar) RETURNS text LANGUAGE sql IMMUTABLE AS $$SELECT 'ALPHA'::text$$",
        "CREATE FUNCTION public.length(varchar) RETURNS int LANGUAGE sql IMMUTABLE AS 'SELECT 99'",
        "CREATE FUNCTION public.count(varchar) RETURNS bigint LANGUAGE sql IMMUTABLE AS 'SELECT 42::bigint'",
        "CREATE FUNCTION public.\"left\"(varchar, int) RETURNS text LANGUAGE sql IMMUTABLE AS $$SELECT 'poison'::text$$",
        "CREATE FUNCTION public.replace(varchar, varchar, varchar) RETURNS text LANGUAGE sql IMMUTABLE AS $$SELECT 'poison'::text$$",
        "CREATE FUNCTION public.substring(varchar, int, int) RETURNS text LANGUAGE sql IMMUTABLE AS $$SELECT 'poison'::text$$",
        "CREATE FUNCTION public.poison_max(cidom, cidom) RETURNS cidom LANGUAGE sql IMMUTABLE AS $$SELECT 'zzz'::cidom$$",
        "CREATE AGGREGATE public.max(cidom) (SFUNC = public.poison_max, STYPE = cidom)",
        # R50: beside the members, in the extension's own schema.
        "CREATE FUNCTION ext.poison_eq(cidom, cidom) RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT true'",
        "CREATE OPERATOR ext.= (LEFTARG = cidom, RIGHTARG = cidom, FUNCTION = ext.poison_eq)",
        "CREATE OPERATOR ext.<> (LEFTARG = cidom, RIGHTARG = cidom, FUNCTION = ext.poison_eq)",
        "CREATE AGGREGATE ext.max(cidom) (SFUNC = public.poison_max, STYPE = cidom)",
        "CREATE FUNCTION ext.replace(cidom, cidom, cidom) RETURNS text LANGUAGE sql IMMUTABLE AS $$SELECT 'poison'::text$$"
      ]
  end

  defp remove_plants(admin) do
    for sql <- [
          "DROP AGGREGATE IF EXISTS ext.max(cidom)",
          "DROP AGGREGATE IF EXISTS public.max(cidom)",
          "DROP FUNCTION IF EXISTS public.poison_max",
          "DROP FUNCTION IF EXISTS public.poison_cat CASCADE",
          "DROP FUNCTION IF EXISTS public.poison_and CASCADE",
          "DROP FUNCTION IF EXISTS ext.poison_eq CASCADE",
          "DROP FUNCTION IF EXISTS ext.replace(cidom, cidom, cidom)"
        ],
        do: exec!(admin, sql)

    for sql <- plants(),
        [_, name] <- [Regex.run(~r/CREATE FUNCTION (public\.\S+?)\(/, sql) || [nil, nil]],
        name != nil do
      exec!(admin, "DROP FUNCTION IF EXISTS #{name} CASCADE")
    end
  end

  defp rel(table, columns, extra \\ %{}) do
    Map.merge(
      %{
        source_table: table,
        primary_key: :id,
        tenant_field: :org,
        fields: Map.keys(columns),
        redact_fields: [],
        columns: columns,
        associations: %{}
      },
      extra
    )
  end

  defp domain do
    types =
      Map.new(~w(org name vc code ci dci path)a, &{&1, %{type: :string}})
      |> Map.merge(Map.new(~w(id n parent_id cat_id)a, &{&1, %{type: :integer}}))
      |> Map.put(:kind, %{type: :string})
      |> Map.put(:ts, %{type: :utc_datetime})
      |> Map.put(:tags, %{type: {:array, :integer}})
      |> Map.put(:lower_vc, %{
        type: :string,
        computed: %{kind: :expression, expression: ["lower", ["field", "vc"]]}
      })

    %{
      name: "poison",
      source:
        rel("items", types, %{
          associations: %{
            cat: %{
              queryable: :cat,
              field: :cat,
              owner_key: :cat_id,
              related_key: :id,
              cardinality: :one
            },
            code_row: %{
              queryable: :code_row,
              field: :code_row,
              owner_key: :ci,
              related_key: :code,
              cardinality: :one
            }
          }
        }),
      schemas: %{
        cat: rel("cats", %{id: %{type: :integer}, org: %{type: :string}, name: %{type: :string}}),
        code_row:
          rel(
            "codes",
            %{code: %{type: :string}, org: %{type: :string}, label: %{type: :string}},
            %{primary_key: :code}
          )
      },
      joins: %{
        cat: %{type: :left, name: "cat"},
        code_row: %{type: :left, name: "code_row"}
      }
    }
  end

  defp base(c, domain \\ domain()) do
    domain
    |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
    |> Selecto.with_tenant("t1")
  end

  defp run(query) do
    case Selecto.execute(query) do
      {:ok, {rows, _columns, _aliases}} -> {:ok, Enum.sort(rows)}
      {:error, %{details: %{reason: reason}}} -> {:refused, reason}
      {:error, other} -> {:error, Exception.message(other)}
    end
  rescue
    exception -> {:raised, Exception.message(exception)}
  end

  defp stream_ids(c) do
    {:ok, stream} =
      c
      |> base()
      |> Selecto.select(["id"])
      |> Selecto.filter({"ci", "mixed"})
      |> Selecto.execute_stream(receive_timeout: 5_000)

    {:ok, stream |> Enum.map(&elem(&1, 0)) |> Enum.sort()}
  end

  defp ids(c, filter) do
    base(c) |> Selecto.select(["id"]) |> Selecto.filter(filter) |> run()
  end

  # The specialised builders: no tenant on these domains (a tenant boundary
  # refuses them, invalid_domain); a required filter scopes the rows.
  defp specialised_domain(joins, associations) do
    columns =
      Map.new(~w(org name vc path)a, &{&1, %{type: :string}})
      |> Map.merge(Map.new(~w(id n parent_id cat_id)a, &{&1, %{type: :integer}}))

    plain = fn table, columns, extra ->
      rel(table, columns, Map.merge(%{tenant_field: nil}, extra))
    end

    named = %{id: %{type: :integer}, org: %{type: :string}, name: %{type: :string}}

    %{
      name: "special",
      source: plain.("items", columns, %{associations: associations}),
      schemas: %{
        cat: plain.("cats", named, %{}),
        label: plain.("labels", named, %{}),
        item: plain.("items", columns, %{})
      },
      joins: joins,
      required_filters: [{"org", "t1"}]
    }
  end

  defp assoc(queryable, field, owner, related),
    do: %{queryable: queryable, field: field, owner_key: owner, related_key: related}

  defp special(c, joins, associations, fields, more \\ & &1) do
    specialised_domain(joins, associations)
    |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
    |> Selecto.select(fields)
    |> more.()
    |> run()
  end

  defp specialised(c) do
    %{
      star_dimension:
        special(
          c,
          %{star: %{type: :star_dimension, name: "star", display_field: :name}},
          %{star: assoc(:cat, :star, :cat_id, :id)},
          ["id", "star.name"]
        ),
      snowflake_dimension:
        special(
          c,
          %{flake: %{type: :snowflake_dimension, name: "flake", display_field: :name}},
          %{flake: assoc(:cat, :flake, :cat_id, :id)},
          ["id", "flake.name"]
        ),
      tagging:
        special(
          c,
          %{
            labels: %{
              type: :tagging,
              name: "labels",
              tag_field: :name
            }
          },
          %{labels: assoc(:label, :labels, :id, :id)},
          ["id", "labels.name"]
        ),
      hierarchy_adjacency:
        special(
          c,
          %{
            tree: %{
              type: :hierarchical,
              name: "tree",
              hierarchy_type: :adjacency_list,
              depth_limit: 4,
              id_field: :id,
              parent_field: :parent_id,
              name_field: :name
            }
          },
          %{tree: assoc(:item, :tree, :parent_id, :id)},
          ["id", "tree.name"]
        ),
      hierarchy_path:
        special(
          c,
          %{
            tree: %{
              type: :hierarchical,
              name: "tree",
              hierarchy_type: :materialized_path,
              path_field: :path,
              path_separator: "/"
            }
          },
          %{tree: assoc(:item, :tree, :parent_id, :id)},
          ["id", "tree.name"]
        ),
      subselect:
        special(
          c,
          %{cat: %{type: :left, name: "cat"}},
          %{cat: assoc(:cat, :cat, :cat_id, :id)},
          ["id"],
          &Selecto.subselect(&1, ["cat.name"])
        )
    }
  end

  defp corpus(c) do
    q = fn fields -> c |> base() |> Selecto.select(fields) end

    filters = [
      plain_eq: {"vc", "alpha"},
      ne: {"vc", {"!=", "alpha"}},
      lt: {"vc", {"<", "b"}},
      in_list: {"vc", ["alpha", "mixed"]},
      not_in: {"vc", {:not_in, ["alpha"]}},
      between: {"n", {:between, 4, 6}},
      starts_with: {"vc", {:starts_with, "al"}},
      like: {"vc", {:like, "al%"}},
      contains: {"vc", {:text_contains, "lph"}},
      ends_with: {"vc", {:ends_with, "ha"}},
      negated: {:not, {"vc", "alpha"}},
      enum_eq: {"kind", "a"},
      domain_eq: {"code", "c3"},
      citext_eq: {"ci", "mixed"},
      citext_ne: {"ci", {"!=", "mixed"}},
      citext_in: {"ci", ["MIXED", "nothing"]},
      citext_like: {"ci", {:like, "MIX%"}},
      citext_lt: {"ci", {"<", "N"}},
      domain_citext_eq: {"dci", "bob"},
      domain_citext_ne: {"dci", {"!=", "BOB"}},
      citext_vs_text: {"ci", {:ref, "name"}},
      text_vs_citext: {"name", {:ref, "ci"}},
      computed_lower: {"lower_vc", "alpha"},
      array_contains: {:array_contains, "tags", [2]},
      array_overlap: {:array_overlap, "tags", [9, 1]}
    ]

    filtered = for {label, filter} <- filters, into: %{}, do: {label, ids(c, filter)}

    filtered
    |> Map.merge(specialised(c))
    |> Map.merge(%{
      tenant: q.(["id"]) |> run(),
      stream: stream_ids(c),
      join: q.(["id", "cat.name"]) |> run(),
      citext_join: q.(["id", "code_row.label"]) |> run(),
      group_count:
        q.(["kind", {:count, "*"}])
        |> Selecto.group_by(["kind"])
        |> run(),
      aggregate_citext_max:
        q.([{:max, "dci"}])
        |> run(),
      lower_select: q.(["id", {:lower, "vc"}]) |> run(),
      upper_select: q.(["id", {:upper, "vc"}]) |> run(),
      length_select: q.(["id", {:length, "vc"}]) |> run(),
      concat_select: q.(["id", {:concat, ["vc", {:literal, "x"}]}]) |> run(),
      coalesce_select: q.(["id", {:coalesce, ["vc", {:literal, "x"}]}]) |> run(),
      window:
        q.(["id"])
        |> Selecto.window_function(:row_number, [],
          over: [partition_by: ["kind"], order_by: ["id"]]
        )
        |> run(),
      subquery_in:
        q.(["id"])
        |> Selecto.filter(
          {"cat_id", {:subquery, :in, "SELECT id FROM cats WHERE name = 'ten'", []}}
        )
        |> run(),
      retarget:
        c
        |> base()
        |> Selecto.retarget("cat", strategy: :in)
        |> Selecto.select(["name"])
        |> run(),
      retarget_exists:
        c
        |> base()
        |> Selecto.retarget("cat", strategy: :exists)
        |> Selecto.select(["name"])
        |> run()
    })
  end

  test "the corpus answers the same under exact-match plants in a pinned schema", %{opts: opts} do
    {:ok, c} = Adapter.connect(opts)

    native = corpus(c)
    GenServer.stop(c)

    assert native.tenant == {:ok, [[1], [3], [5]]}
    assert native.citext_eq == {:ok, [[1], [3], [5]]}
    assert native.citext_ne == {:ok, []}
    assert native.domain_citext_eq == {:ok, [[1], [3]]}
    assert native.citext_vs_text == {:ok, [[3]]}
    assert native.text_vs_citext == {:ok, [[3]]}
    assert native.citext_join == {:ok, [[1, "tag-mixed"], [3, "tag-mixed"], [5, "tag-mixed"]]}

    assert native.aggregate_citext_max == {:ok, [["Bob"]]} or
             native.aggregate_citext_max == {:ok, [["BOB"]]}

    bad = for {label, result} <- native, not match?({:ok, _}, result), do: {label, result}
    assert bad == []

    {:ok, admin} = Adapter.connect(opts)
    for sql <- plants(), do: exec!(admin, sql)

    try do
      # Reconnect: the plants are in place for a fresh type cache too.
      {:ok, c} = Adapter.connect(opts)
      poisoned = corpus(c)

      hijacked =
        for {label, result} <- native,
            Map.fetch!(poisoned, label) != result,
            do: {label, result, Map.fetch!(poisoned, label)}

      assert hijacked == []

      # The plants do win a hand-written statement.
      assert %{rows: [["alpha"]]} =
               exec!(c, "SELECT lower(CAST('SECRET' AS varchar))")

      GenServer.stop(c)
    after
      remove_plants(admin)
      GenServer.stop(admin)
    end
  end

  # Hand-built reads through the guarded path: the over-refusals Perl's
  # hotfix rounds shipped, and R49c's type-following expressions.
  @reads [
    {"citext aggregate", "SELECT max(ci) FROM items WHERE org = 't1'"},
    {"citext aggregate over a domain", "SELECT max(dci) FROM items WHERE org = 't1'"},
    {"citext aggregate with DISTINCT", "SELECT max(DISTINCT ci) FROM items WHERE org = 't1'"},
    {"implicit alias through a derived table",
     "SELECT s.id FROM (SELECT id, ci c FROM items) s WHERE s.c = 'MIXED' ORDER BY s.id"},
    {"citext aggregate in HAVING",
     "SELECT id FROM items WHERE org = 't1' GROUP BY id HAVING max(ci) = 'MIXED' ORDER BY id"},
    {"pg_catalog SRF in FROM", "SELECT g FROM generate_series(1, 3) AS g"},
    {"pg_catalog SRF in the select list", "SELECT unnest(tags) FROM items WHERE id = 1"},
    {"function beside a citext overload", "SELECT x FROM regexp_split_to_table('a,b', ',') AS x"},
    {"VARIADIC concat", "SELECT concat(vc, 'x', 1) FROM items WHERE id = 1"},
    {"VARIADIC format", "SELECT format('%s-%s', vc, n) FROM items WHERE id = 1"},
    {"VARIADIC json_build_object",
     "SELECT json_build_object('a', vc)::text FROM items WHERE id = 1"},
    {"extension-only function", "SELECT similarity(name, 'alpha') FROM items WHERE id = 1"},
    {"lower of citext", "SELECT lower(ci) FROM items WHERE id = 1"},
    {"searched CASE over citext",
     "SELECT id FROM items WHERE CASE WHEN ci = 'MIXED' THEN 1 ELSE 0 END = 1 ORDER BY id"},
    {"simple CASE over citext",
     "SELECT id FROM items WHERE CASE ci WHEN 'MIXED' THEN 1 ELSE 0 END = 1 ORDER BY id"},
    {"NULLIF over citext", "SELECT NULLIF(ci, 'MIXED') FROM items WHERE id = 3"},
    {"IS DISTINCT FROM over citext",
     "SELECT id FROM items WHERE ci IS NOT DISTINCT FROM 'MIXED' ORDER BY id"},
    {"IN and BETWEEN over citext",
     "SELECT id FROM items WHERE ci IN ('MIXED', 'x') AND ci BETWEEN 'A' AND 'Z' ORDER BY id"},
    {"member citext through a CTE",
     "WITH m AS (SELECT id, ci AS c FROM items) SELECT id FROM m WHERE c = 'MIXED' ORDER BY id"},
    {"member citext through a lateral",
     "SELECT i.id FROM items i JOIN LATERAL (SELECT j.ci FROM items j WHERE j.id = i.id) l " <>
       "ON l.ci = 'MIXED' ORDER BY i.id"},
    {"aliased member: the alias's own type",
     "SELECT s.id FROM (SELECT id, ci AS x, name AS ci FROM items) s WHERE s.ci = 'MIXED'"},
    {"reused alias, own scope",
     "SELECT r.id FROM items r WHERE r.ci = 'MIXED' AND EXISTS " <>
       "(SELECT 1 FROM cats r2 WHERE r2.id = r.cat_id AND r2.name = 'TEN') ORDER BY r.id"},
    {"domain over citext",
     "SELECT id FROM items WHERE dci = 'bob' AND dci <> 'BOBBY' ORDER BY id"},
    {"cast to citext", "SELECT id FROM items WHERE CAST(vc AS ext.citext) = 'MIXED' ORDER BY id"},
    {"citext against text", "SELECT id FROM items WHERE ci = name ORDER BY id"},
    # M1: `name =` after a comma is an assignment only in a SET list.
    {"comparison after a comma in the select list",
     "SELECT id, vc = 'zzz' FROM items ORDER BY id"},
    {"comparison in a call's second argument",
     "SELECT id FROM items WHERE coalesce(null, vc = 'zzz') ORDER BY id"},
    {"comparison after a comma in ORDER BY", "SELECT id FROM items ORDER BY id, vc = 'zzz'"},
    {"comparison in an ARRAY", "SELECT ARRAY[null, vc = 'zzz'] FROM items ORDER BY id"},
    {"mode() is a call", "SELECT mode() WITHIN GROUP (ORDER BY vc) FROM items"},
    # M2.
    {"dollar quotes", "SELECT id FROM items WHERE vc = $$alpha$$"},
    {"tagged dollar quotes", "SELECT id FROM items WHERE vc = $t$alpha$t$ ORDER BY id"},
    {"escape string", "SELECT id FROM items WHERE vc = E'al\\x70ha'"},
    {"unicode escape string", "SELECT id FROM items WHERE vc = U&'\\0061lpha'"},
    {"string continuation", "SELECT id FROM items WHERE vc = 'alp'\n 'ha'"},
    {"overloaded extension function",
     "SELECT encode(digest(name, 'sha1'), 'hex') FROM items WHERE id = 1"},
    {"extension function result's operator", "SELECT hstore('a'::text, '1'::text) -> 'a'"},
    # M3: citext answered natively, or refused (the next test).
    {"ALL over a citext array",
     "SELECT id FROM items WHERE ci <> ALL(ARRAY['MIXED']::ext.citext[]) ORDER BY id"},
    {"ANY over a citext array",
     "SELECT id FROM items WHERE ci = ANY(ARRAY['MIXED']::ext.citext[]) ORDER BY id"},
    {"scalar citext subquery",
     "SELECT id FROM items WHERE ci <> (SELECT 'MIXED'::ext.citext) ORDER BY id"},
    {"scalar text subquery",
     "SELECT id FROM items WHERE ci = (SELECT 'mixed'::text) ORDER BY id"},
    {"IN over a text subquery",
     "SELECT id FROM items WHERE ci IN (SELECT name FROM items) ORDER BY id"},
    {"NOT IN over a citext subquery",
     "SELECT id FROM items WHERE ci NOT IN (SELECT code FROM codes) ORDER BY id"},
    {"citext split_part", "SELECT split_part(ci, 'X', 1) FROM items WHERE id = 3"},
    {"citext strpos", "SELECT strpos(ci, 'M') FROM items WHERE id = 3"},
    {"citext rowset beside a table",
     "SELECT x FROM items, regexp_split_to_table(ci, 'I') AS x WHERE id = 3 ORDER BY 1"},
    {"citext LIKE a pattern", "SELECT id FROM items WHERE ci LIKE 'MIX%' ORDER BY id"},
    {"text against citext", "SELECT id FROM items WHERE name = ci ORDER BY id"}
  ]

  test "hand-built reads answer as natively (R68)", %{opts: opts} do
    {:ok, setup} = Adapter.connect(opts)
    exec!(setup, "CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA ext")
    GenServer.stop(setup)

    {:ok, c} = Adapter.connect(opts)

    for {label, sql} <- @reads do
      assert {:ok, %{rows: native}} = Adapter.execute(c, sql, [], [])
      assert {:ok, %{rows: ^native}} = Adapter.execute(c, sql, [], selecto_read: true), label
    end

    GenServer.stop(c)
  end

  # The qualifier's own route, what Selecto generates: hand-built text runs as
  # written.
  defp typed(c, sql, tables \\ ["items"]) do
    relations =
      Map.new(tables, &{&1, ~s("public"."#{&1}")})
      |> Map.put(SelectoDBPostgreSQL.RelationPath.path_key(), ["public", "ext"])

    Adapter.execute(c, sql, [],
      selecto_read: true,
      selecto_generated: true,
      selecto_qualified_relations: relations
    )
  end

  defp summary({:ok, %{rows: rows}}), do: rows
  defp summary({:error, %{details: details} = error}), do: {details[:reason], error.message}
  defp summary(other), do: other

  test "citext overloads answer natively; with a plant in reach they refuse (R58, R62)", %{
    opts: opts
  } do
    {:ok, c} = Adapter.connect(opts)

    assert %{rows: [["zixed"]]} =
             guarded_rows(c, "SELECT replace(ci, 'M', 'z') FROM items WHERE id = 3")

    assert %{rows: [["zed"]]} =
             guarded_rows(c, "SELECT regexp_replace(ci, 'MIX', 'z') FROM items WHERE id = 3")

    # Two overloads an untyped pair could take: PostgreSQL's choice, not ours.
    assert {:error, %{details: %{reason: :function_ambiguous}}} =
             typed(c, "SELECT hstore('a', '1')::text")

    GenServer.stop(c)

    {:ok, admin} = Adapter.connect(opts)
    for sql <- plants(), do: exec!(admin, sql)

    try do
      {:ok, c} = Adapter.connect(opts)

      assert {:error, %{details: %{reason: :function_ambiguous}}} =
               typed(c, "SELECT replace(ci, 'M', 'z') FROM items WHERE id = 3")

      GenServer.stop(c)
    after
      remove_plants(admin)
      GenServer.stop(admin)
    end
  end

  defp guarded_rows(c, sql) do
    {:ok, result} = Adapter.execute(c, sql, [], selecto_read: true)
    result
  end

  test "an expression that may be citext refuses against an untyped value (R49c)", %{opts: opts} do
    {:ok, c} = Adapter.connect(opts)

    # PostgreSQL's common type names these: citext throughout, as natively.
    for sql <- [
          "SELECT id FROM items WHERE COALESCE(ci, 'x') = 'MIXED' ORDER BY id",
          "SELECT id FROM items WHERE (CASE WHEN n > 0 THEN ci END) = 'MIXED' ORDER BY id",
          "SELECT id FROM items WHERE GREATEST(ci, 'a') = 'MIXED' ORDER BY id",
          "SELECT max(COALESCE(ci, 'x')) FROM items"
        ] do
      assert {:ok, %{rows: rows}} = typed(c, sql), sql
      assert {:ok, %{rows: ^rows}} = Adapter.execute(c, sql, [], []), sql
    end

    for sql <- [
          # M3: no OPERATOR() says "each column by its own type".
          "SELECT id FROM items WHERE (ci, org) <> ('MIXED', 't1')",
          "SELECT id FROM items WHERE ROW(ci, org) = ROW('MIXED', 't1')"
        ] do
      assert {:error, %{details: %{reason: :operand_type_unknown}}} =
               typed(c, sql),
             sql
    end

    # A computed field over citext and varchar through the domain: their common
    # type is text, as natively.
    computed =
      domain()
      |> put_in([:source, :columns, :ci_or], %{
        type: :string,
        computed: %{kind: :expression, expression: ["coalesce", ["field", "ci"], ["field", "vc"]]}
      })
      |> update_in([:source, :fields], &[:ci_or | &1])

    assert {:ok, []} =
             c
             |> base(computed)
             |> Selecto.select(["id"])
             |> Selecto.filter({"ci_or", "MIXED"})
             |> run()

    GenServer.stop(c)
  end

  test "a function outside the pinned schemas is not a candidate", %{opts: opts} do
    {:ok, admin} = Adapter.connect(opts)

    for sql <- [
          "CREATE SCHEMA offpath",
          "CREATE FUNCTION offpath.only_there(text) RETURNS text LANGUAGE sql IMMUTABLE AS 'SELECT $1'"
        ],
        do: exec!(admin, sql)

    try do
      {:ok, c} = Adapter.connect(opts)

      assert {:error, %{details: %{reason: :function_not_found}}} =
               typed(c, "SELECT only_there(name) FROM items")

      # Hand-built text gets the driver's own answer under the pin.
      assert {:error, %Postgrex.Error{postgres: %{code: :undefined_function}}} =
               Adapter.execute(c, "SELECT only_there(name) FROM items", [], selecto_read: true)

      GenServer.stop(c)
    after
      exec!(admin, "DROP SCHEMA offpath CASCADE")
      GenServer.stop(admin)
    end
  end

  test "U& and dollar-quoted strings compare as untyped literals, as natively", %{opts: opts} do
    {:ok, c} = Adapter.connect(opts)

    for literal <- [
          "'mixed'",
          "E'mixed'",
          "U&'mixed'",
          "U&'m!0069xed' UESCAPE '!'",
          "$$mixed$$",
          "$q$mixed$q$"
        ] do
      sql = "SELECT id FROM items WHERE ci = #{literal} ORDER BY id"
      assert {:ok, %{rows: [[1], [3], [5]]}} = Adapter.execute(c, sql, [], selecto_read: true)
    end

    assert {:ok, %{rows: [[2]]}} =
             Adapter.execute(c, "SELECT id FROM items WHERE ci NOT IN (U&'MIXED')", [],
               selecto_read: true
             )

    assert {:ok, %{rows: [[1]]}} =
             Adapter.execute(
               c,
               "SELECT id FROM items WHERE name LIKE ANY (ARRAY['al%']) ORDER BY id",
               [],
               selecto_read: true
             )

    GenServer.stop(c)
  end

  test "citext's pattern operators answer for every pg_catalog string (M3, R51)", %{opts: opts} do
    {:ok, c} = Adapter.connect(opts)
    read = &typed(c, &1)
    mixed = [[1], [3], [5]]

    for sql <- [
          "SELECT id FROM items WHERE ci LIKE 'mixed'::varchar ORDER BY id",
          "SELECT id FROM items WHERE ci ~~ CAST('mixed' AS varchar) ORDER BY id",
          "SELECT id FROM items WHERE ci ~ 'mixed'::varchar ORDER BY id",
          "SELECT id FROM items WHERE ci LIKE N'mixed' ORDER BY id",
          "SELECT id FROM items WHERE ci SIMILAR TO 'mixed' ORDER BY id",
          "SELECT id FROM items WHERE ci LIKE ANY (ARRAY['mixed', 'zzz']) ORDER BY id",
          "SELECT id FROM items WHERE ci LIKE ANY (ARRAY['mixed']::text[]) ORDER BY id",
          "SELECT id FROM items WHERE (ci || 'x') = 'MiXedx' OR (ci || 'x') = 'mixedx' ORDER BY id"
        ] do
      assert {:ok, %{rows: ^mixed}} = read.(sql), sql
    end

    for sql <- [
          "SELECT id FROM items WHERE ci LIKE lower(vc) ORDER BY id",
          "SELECT id FROM items WHERE ci LIKE NULLIF(lower(vc), '') ORDER BY id",
          "SELECT id FROM items WHERE ci LIKE (SELECT 'mixed') ORDER BY id",
          "SELECT id FROM items WHERE ci LIKE (SELECT 'mixed'::text) ORDER BY id",
          "SELECT id FROM items WHERE ci LIKE CASE WHEN id > 0 THEN 'mixed'::text ELSE vc END ORDER BY id",
          "SELECT id FROM items WHERE ci LIKE ('mix' || 'ed') ORDER BY id"
        ] do
      assert {:ok, %{rows: rows}} = read.(sql), sql
      assert {:ok, %{rows: ^rows}} = Adapter.execute(c, sql, [], []), sql
    end

    # As text: case-sensitive, as natively.
    starts = "SELECT id FROM items WHERE ci ^@ 'mix'::ext.citext ORDER BY id"
    assert {:ok, %{rows: [[3], [5]]}} = read.(starts)
    assert {:ok, %{rows: [[3], [5]]}} = Adapter.execute(c, starts, [], [])
    assert {:ok, %{rows: [_ | _]}} = read.("SELECT id FROM items WHERE (ci || ci) <> 'x'")

    assert {:ok, %{rows: [[2]]}} = read.("SELECT id FROM items WHERE ci NOT SIMILAR TO 'mixed'")
    assert {:ok, %{rows: [[2]]}} = read.("SELECT id FROM items WHERE ci !~ 'mixed'::varchar")
    assert {:ok, %{rows: []}} = read.(~S|SELECT id FROM items WHERE ci = E'mi\'xed'|)
    GenServer.stop(c)
  end

  test "NULLIF and a subscript take the type they carry (F2)", %{opts: opts} do
    {:ok, c} = Adapter.connect(opts)
    sql = fn where -> "SELECT id FROM items WHERE #{where} ORDER BY id" end

    for where <- [
          "NULLIF(ci, 'x') = 'MIXED'",
          "NULLIF(ci, 'x') <> 'MIXED'",
          "NULLIF(ci, 'x') LIKE 'MIX%'",
          "NULLIF(ci, 'x') NOT LIKE 'MIX%'",
          "NULLIF(NULLIF(ci, 'x'), 'y') = 'MIXED'",
          "NULLIF(dci, 'x') = 'BOB'",
          "NULLIF('mixed', ci) IS NULL",
          "NULLIF(ci, vc) = 'MIXED'",
          "COALESCE(NULLIF(ci, 'x'), 'y') = 'MIXED'"
        ] do
      statement = sql.(where)
      assert {:ok, %{rows: rows}} = typed(c, statement), where
      assert {:ok, %{rows: ^rows}} = Adapter.execute(c, statement, [], []), where
    end

    # An array no column names has an element type not known here: refused.
    for where <- ["(ARRAY['a'::ext.citext])[1] = 'A'"] do
      assert {:error, %{details: %{reason: :operand_type_unknown}}} =
               typed(c, sql.(where)),
             where
    end

    GenServer.stop(c)
  end

  test "hand-built SQL touching an extension type runs as written (R68)", %{opts: opts} do
    {:ok, c} = Adapter.connect(opts)

    statements =
      for where <- [
            "ci::text = 'x'",
            "CAST(ci AS text) = 'x'",
            "COALESCE(ci, 'x') = 'x'",
            "NULLIF(ci, 'x') = 'x'",
            "GREATEST(ci, 'x') = 'x'",
            "LEAST(ci, 'x') = 'x'",
            "CASE WHEN id > 0 THEN ci END = 'x'",
            "ci = (SELECT 'x')",
            "id IN (SELECT id FROM items WHERE ci = 'x')",
            "('MIXED') = ci",
            "ci = 'MIXED'",
            "ci = 'ABC' COLLATE \"C\"",
            "(ci, org) <> ('MIXED', 't1')"
          ] do
        "SELECT id FROM items WHERE #{where} ORDER BY id"
      end ++
        [
          "SELECT ci FROM items UNION SELECT ci FROM items ORDER BY 1",
          "WITH q AS (SELECT ci FROM items) SELECT ci FROM q ORDER BY 1",
          "SELECT (ARRAY[ci])[1] FROM items ORDER BY 1",
          "SELECT (SELECT 1)::int FROM cats WHERE id = 10"
        ]

    for sql <- statements do
      assert {:ok, %{rows: native}} = Adapter.execute(c, sql, [], [])
      assert {:ok, %{rows: ^native}} = Adapter.execute(c, sql, [], selecto_read: true), sql
    end

    GenServer.stop(c)
  end

  test "hand-built text must be a read, and its writes refuse (R68)", %{opts: opts} do
    {:ok, c} = Adapter.connect(opts)
    read = &Adapter.execute(c, &1, [], selecto_read: true)

    for sql <- [
          "COMMIT",
          "ROLLBACK",
          "DELETE FROM items",
          "SET search_path = ext",
          "EXPLAIN SELECT 1",
          "/* SELECT */ COMMIT",
          "-- SELECT\nCOMMIT",
          "/* /* */ SELECT */ COMMIT",
          "(COMMIT)",
          "SELECTED",
          "",
          "/* never closed"
        ] do
      assert {:error, %{details: %{reason: :hand_built_not_a_read}}} = read.(sql), sql
    end

    # Comments nest on the server: a word inside one is no first word.
    assert {:ok, %{rows: [[1]]}} = read.("/* /* a */ b */ -- c\n ((SELECT 1))")
    assert {:ok, %{rows: [[";"]]}} = read.("SELECT ';'")

    for sql <- [
          "WITH d AS (DELETE FROM items RETURNING id) SELECT id FROM d",
          "SELECT id FROM items FOR UPDATE",
          "SELECT id INTO TEMP selecto_into FROM items"
        ] do
      assert {:error, %{details: %{reason: :read_only_read}}} = read.(sql), sql
    end

    # One statement is the driver's rule: nothing can follow the read.
    assert {:error, %Postgrex.Error{postgres: %{code: :syntax_error}}} =
             read.("SELECT 1; DELETE FROM items")

    assert %{rows: [[4]]} = exec!(c, "SELECT count(*) FROM items")
    GenServer.stop(c)
  end

  test "generated citext forms answer as PostgreSQL does (tenth review M2)", %{opts: opts} do
    {:ok, c} = Adapter.connect(opts)

    for sql <- [
          "SELECT id FROM items WHERE ci @@ websearch_to_tsquery('mixed') ORDER BY id",
          "SELECT min(greatest(upper(ci), ci)) FROM items",
          "SELECT min(coalesce(upper(ci), ci)) FROM items",
          "SELECT max(CASE WHEN id > 1 THEN upper(ci) ELSE ci END) FROM items",
          "SELECT id FROM items WHERE greatest(upper(ci), ci) = 'MIXED' ORDER BY id",
          "SELECT max(coalesce(nullif('', 'a%'), ci)) FROM items",
          "SELECT min(least(nullif('a%', 'x'), ci)) FROM items",
          "WITH q AS (SELECT *, 1 AS depth FROM items) SELECT id FROM q WHERE ci = 'mixed' ORDER BY id",
          "WITH q AS (SELECT items.*, 1 AS depth FROM items) SELECT id FROM q WHERE ci = 'mixed' ORDER BY id"
        ] do
      assert {:ok, %{rows: rows}} = Adapter.execute(c, sql, [], [])
      assert {:ok, %{rows: ^rows}} = typed(c, sql), sql
    end

    GenServer.stop(c)
  end

  test "a cancel during a catalog or session read stands as itself (tenth review M3)" do
    canceled = {:error, %Postgrex.Error{postgres: %{code: :query_canceled, pg_code: "57014"}}}
    lost = {:error, %DBConnection.ConnectionError{message: "closed"}}

    for failure <- [canceled, lost] do
      execute = fn _sql, _params -> failure end

      assert SelectoDBPostgreSQL.RelationPath.resolve(execute, ["t"], false) == failure
      assert SelectoDBPostgreSQL.RelationPath.resolve(execute, ["t"], true) == failure
      assert SelectoDBPostgreSQL.SessionGuard.pin(execute, "pg_catalog") == failure
      assert ExplicitSQL.prepare(execute, "SELECT lower(name) FROM items") == failure
    end

    # Anything else is still the typed refusal.
    other = fn _sql, _params -> {:error, %Postgrex.Error{postgres: %{code: :undefined_table}}} end

    assert {:error, %{details: %{reason: :relation_unresolved}}} =
             SelectoDBPostgreSQL.RelationPath.resolve(other, ["t"], false)
  end

  test "a failed RELEASE SAVEPOINT undoes the write (R70)" do
    released = fn sql, answers ->
      if String.starts_with?(sql, "RELEASE"),
        do: Agent.get_and_update(answers, fn [a | rest] -> {a, rest} end),
        else: {:ok, :sent}
    end

    failed = {:error, %Postgrex.Error{postgres: %{code: :query_canceled}}}

    # The savepoint goes, and the write is reported failed.
    {:ok, answers} = Agent.start_link(fn -> [failed, {:ok, :released}] end)
    sent = :ets.new(:sent, [:public, :ordered_set])

    query = fn sql ->
      :ets.insert(sent, {System.unique_integer([:monotonic]), sql})
      released.(sql, answers)
    end

    assert {:error, %{type: :transaction_failed}} =
             Adapter.with_savepoint(query, fn -> {:ok, :written} end)

    assert [{_, "SAVEPOINT " <> name}, {_, "RELEASE " <> _}, {_, rollback}, {_, _release}] =
             :ets.tab2list(sent)

    assert rollback == "ROLLBACK TO SAVEPOINT " <> name

    # The undo fails as well: the caller is told the connection is broken.
    undo_fails = fn sql ->
      if String.starts_with?(sql, "SAVEPOINT"), do: {:ok, :sent}, else: failed
    end

    assert {:error, %{details: %{connection_broken: true}}} =
             Adapter.with_savepoint(undo_fails, fn -> {:ok, :written} end)
  end

  test "text without the executor's stamp is hand-built, whatever it is passed with", %{
    opts: opts
  } do
    {:ok, c} = Adapter.connect(opts)
    sql = "SELECT id FROM public.items WHERE (ci, org) = ('MIXED', 't1') ORDER BY id"
    relations = %{SelectoDBPostgreSQL.RelationPath.path_key() => ["public", "ext"]}
    read = &Adapter.execute(c, sql, [], [selecto_read: true] ++ &1)

    assert {:ok, %{rows: rows}} = Adapter.execute(c, sql, [], [])
    assert {:ok, %{rows: ^rows}} = read.(selecto_qualified_relations: relations)
    assert {:ok, %{rows: ^rows}} = read.(selecto_generated: true)

    assert {:error, %{details: %{reason: :operand_type_unknown}}} =
             read.(selecto_generated: true, selecto_qualified_relations: relations)

    GenServer.stop(c)
  end

  test "raw SQL in a generated read runs as written, the rest of it as compiled (R68)", %{
    opts: opts
  } do
    {:ok, c} = Adapter.connect(opts)

    raw = fn filter ->
      c |> base(domain()) |> Selecto.select(["id"]) |> Selecto.filter(filter) |> run()
    end

    native = fn where ->
      exec!(c, "SELECT id FROM items WHERE org = 't1' AND #{where} ORDER BY id").rows
      |> Enum.sort()
    end

    sorted = fn {:ok, rows} -> Enum.sort(rows) end

    assert sorted.(raw.({:raw_sql_filter, ["ci::text = 'MIXED'"]})) ==
             native.("ci::text = 'MIXED'")

    assert sorted.(raw.({:raw_sql_filter, ["COALESCE(ci, 'x') = 'mixed'"]})) ==
             native.("COALESCE(ci, 'x') = 'mixed'")

    assert sorted.(raw.({:exists, "SELECT 1 FROM items x WHERE x.ci = 'mixed'"})) ==
             native.("EXISTS (SELECT 1 FROM items x WHERE x.ci = 'mixed')")

    assert {:ok, _} = raw.({"ci", {:subquery, :in, "SELECT code FROM codes", []}})
    GenServer.stop(c)
  end

  test "custom_sql templates run as written (M3, R68)", %{opts: opts} do
    {:ok, c} = Adapter.connect(opts)
    read = fn selector -> c |> base(domain()) |> Selecto.select(["id", selector]) |> run() end

    assert {:ok, rows} = read.({:custom_sql, "upper({{f}})", %{"f" => "name"}})
    assert length(rows) == 3

    assert {:ok, rows} = read.({:custom_sql, "COALESCE({{f}}, 'x')", %{"f" => "ci"}})
    native = exec!(c, "SELECT id, COALESCE(ci, 'x') FROM items WHERE org = 't1'").rows
    assert Enum.sort(rows) == Enum.sort(native)

    GenServer.stop(c)
  end

  test "a cast literal is typed, parenthesised or not", %{opts: opts} do
    {:ok, c} = Adapter.connect(opts)
    sql = "SELECT id FROM items WHERE ('mixed')::text = ci ORDER BY id"
    assert {:ok, %{rows: rows}} = Adapter.execute(c, sql, [], [])
    assert {:ok, %{rows: ^rows}} = typed(c, sql)
    GenServer.stop(c)
  end

  test "generated NULLIF coerces its first operand, whatever its form (H2)", %{opts: opts} do
    {:ok, c} = Adapter.connect(opts)

    for sql <- [
          "SELECT NULLIF(COALESCE(ci, CAST('a' AS text)), name), count(*) FROM items GROUP BY 1 ORDER BY 1",
          "SELECT NULLIF(GREATEST(ci, ci), upper('')), count(*) FROM items GROUP BY 1 ORDER BY 1",
          "SELECT NULLIF(NULLIF(ci, 'x'), name), count(*) FROM items GROUP BY 1 ORDER BY 1",
          "SELECT NULLIF(CASE WHEN id > 0 THEN ci END, lower(name)), count(*) FROM items GROUP BY 1 ORDER BY 1"
        ] do
      assert {:ok, %{rows: rows}} = Adapter.execute(c, sql, [], [])
      assert {:ok, %{rows: ^rows}} = typed(c, sql), sql
    end

    GenServer.stop(c)
  end

  test "NULLIF of citext and text is text on the generated route (M5)", %{opts: opts} do
    {:ok, c} = Adapter.connect(opts)
    sql = "SELECT min(NULLIF(ci, name)) FROM items"
    assert {:ok, %{rows: native}} = Adapter.execute(c, sql, [], [])
    assert {:ok, %{rows: ^native}} = typed(c, sql)

    assert {:ok, %{rows: flipped}} =
             Adapter.execute(c, "SELECT min(NULLIF('z', ci)) FROM items", [], [])

    assert {:ok, %{rows: ^flipped}} = typed(c, "SELECT min(NULLIF('z', ci)) FROM items")

    for sql <- [
          "SELECT NULLIF(random(), 0.5) FROM items",
          "SELECT CASE random() WHEN 0.5 THEN 1 END FROM items"
        ] do
      assert {:error, %{details: %{reason: :volatile_operand}}} = typed(c, sql), sql
    end

    GenServer.stop(c)
  end

  test "a statement beyond the bound refuses, whatever its shape (M6)", %{opts: opts} do
    {:ok, c} = Adapter.connect(opts)
    nested = Enum.reduce(1..40, "name", fn _n, inner -> "NULLIF(#{inner}, 'x')" end)

    cased =
      Enum.reduce(1..40, "name", fn _n, inner ->
        "CASE #{inner} WHEN 'a' THEN 'b' WHEN 'c' THEN 'd' END"
      end)

    wide = Enum.map_join(1..30_000, " OR ", &"id = #{&1}")

    for sql <- [
          "SELECT id FROM items WHERE #{nested} = 'a'",
          "SELECT id FROM items WHERE #{cased} = 'a'"
        ] do
      {micros, result} = :timer.tc(fn -> typed(c, sql) end)
      assert {:error, %{details: %{reason: :statement_too_large}}} = result
      assert micros < 30_000_000
    end

    # Hand-built text is the server's to bound, and it answers.
    for sql <- [
          "SELECT id FROM items WHERE #{wide} ORDER BY id",
          "SELECT id FROM items WHERE name = '#{String.duplicate("a", 3_000_000)}'"
        ] do
      assert {:ok, %{rows: rows}} = Adapter.execute(c, sql, [], [])
      assert {:ok, %{rows: ^rows}} = Adapter.execute(c, sql, [], selecto_read: true)
    end

    GenServer.stop(c)
  end

  test "a plant under a keyword or U& name never answers (R58, R60)", %{opts: opts} do
    {:ok, admin} = Adapter.connect(opts)
    forged = "RETURNS text LANGUAGE sql AS $$ SELECT 'FORGED'::text $$"

    for sql <- [
          "CREATE FUNCTION public.explain(int) #{forged}",
          "CREATE FUNCTION public.cube(int, int) #{forged}",
          "CREATE FUNCTION public.lower(varchar) #{forged}"
        ],
        do: exec!(admin, sql)

    try do
      {:ok, c} = Adapter.connect(opts)
      read = &typed(c, &1)

      for sql <- [
            "SELECT explain(n) FROM items",
            "SELECT cube(n, n) FROM items",
            "SELECT public.cube(n, n) FROM items",
            ~s|SELECT U&"cube"(n, n) FROM items|
          ] do
        assert {:error, %{details: %{reason: :function_not_member}}} = read.(sql)
      end

      assert {:ok, %{rows: [["alpha"]]}} =
               read.(~s|SELECT U&"lower"(vc) FROM items WHERE id = 1|)

      assert {:ok, %{rows: rows}} =
               read.("SELECT n, count(*) FROM items GROUP BY CUBE (n) ORDER BY n")

      assert length(rows) == 5
      GenServer.stop(c)
    after
      for sql <- [
            "DROP FUNCTION public.explain(int)",
            "DROP FUNCTION public.cube(int, int)",
            "DROP FUNCTION public.lower(varchar)"
          ],
          do: exec!(admin, sql)

      GenServer.stop(admin)
    end
  end

  test "a domain chain deeper than the walk refuses", %{opts: opts} do
    {:ok, admin} = Adapter.connect(opts)

    domains =
      Enum.map_reduce(1..17, "ext.citext", fn level, previous ->
        {"CREATE DOMAIN deep#{level} AS #{previous}", "deep#{level}"}
      end)
      |> elem(0)

    for sql <- domains, do: exec!(admin, sql)
    exec!(admin, "CREATE TABLE deep16t (id int PRIMARY KEY, k deep16)")
    exec!(admin, "CREATE TABLE deep17t (id int PRIMARY KEY, k deep17)")
    exec!(admin, "INSERT INTO deep16t VALUES (1, 'MiX')")

    try do
      {:ok, c} = Adapter.connect(opts)

      assert {:ok, %{rows: [[1]]}} =
               typed(c, "SELECT id FROM deep16t WHERE k = 'mix'", ["deep16t"])

      assert {:error, %{details: %{reason: :domain_chain_too_deep}}} =
               typed(c, "SELECT id FROM deep17t WHERE k = 'mix'", ["deep17t"])

      GenServer.stop(c)
    after
      exec!(admin, "DROP TABLE deep16t, deep17t")
      exec!(admin, "DROP DOMAIN deep1 CASCADE")
      GenServer.stop(admin)
    end
  end

  test "a citext-tenant UPDATE applies, under plants, and another tenant's never", %{opts: opts} do
    {:ok, admin} = Adapter.connect(opts)

    for sql <- [
          "CREATE TABLE ct (id int PRIMARY KEY, org ext.citext NOT NULL, site varchar(5), note text)",
          "INSERT INTO ct VALUES (1, 't1', 's', 'a'), (2, 't2', 's', 'SECRET')"
        ],
        do: exec!(admin, sql)

    for sql <- plants(), do: exec!(admin, sql)

    try do
      {:ok, c} = Adapter.connect(opts)
      scope = {:eq, {:field, "org"}, {:literal, "t1"}}

      update = fn id ->
        {:ok, command} =
          Command.new(%{
            operation: :update,
            relation: "ct",
            predicate:
              {:and,
               [
                 {:eq, {:field, "id"}, {:literal, id}},
                 {:eq, {:field, "site"}, {:literal, "s"}},
                 scope
               ]},
            assignments: [%{field: "note", value: {:literal, "new"}}],
            expected_cardinality: :many,
            metadata: %{post_image: %{predicate: scope, key: ["id"]}}
          })

        Adapter.execute_write_unsafe(c, command, [])
      end

      assert {:ok, %{affected_rows: 1}} = update.(1)
      assert {:ok, %{affected_rows: 0}} = update.(2)
      assert %{rows: [["new"], ["SECRET"]]} = exec!(c, "SELECT note FROM ct ORDER BY id")
      GenServer.stop(c)
    after
      remove_plants(admin)
      exec!(admin, "DROP TABLE ct")
      GenServer.stop(admin)
    end
  end

  test "R49a: a column type changed after a REPEATABLE READ snapshot refuses", %{opts: opts} do
    {:ok, admin} = Adapter.connect(opts)

    exec!(
      admin,
      "CREATE TABLE rr (id int PRIMARY KEY, org varchar(5) NOT NULL, k text, other text)"
    )

    exec!(admin, "INSERT INTO rr VALUES (1, 't1', 'MiX', 'x')")

    try do
      {:ok, c} = Adapter.connect(opts)

      in_snapshot = fn fun ->
        Postgrex.transaction(c, fn conn ->
          Postgrex.query!(conn, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", [])
          Postgrex.query!(conn, "SELECT 1", [])
          fun.(conn)
          Postgrex.rollback(conn, :done)
        end)
      end

      read = &typed(&1, &2, ["rr"])

      # Nothing changed: the read runs.
      in_snapshot.(fn conn ->
        assert {:ok, %{rows: [[1]]}} = read.(conn, "SELECT id FROM rr WHERE k = 'MiX'")
      end)

      # A column the statement does not name is not consulted.
      in_snapshot.(fn conn ->
        exec!(admin, "ALTER TABLE rr ALTER COLUMN other TYPE ext.citext")
        assert {:ok, %{rows: [[1]]}} = read.(conn, "SELECT id FROM rr WHERE k = 'MiX'")
      end)

      in_snapshot.(fn conn ->
        exec!(admin, "ALTER TABLE rr ALTER COLUMN k TYPE ext.citext")

        assert {:error, %{details: %{reason: :catalog_changed}}} =
                 read.(conn, "SELECT id FROM rr WHERE k = 'mix'")
      end)

      GenServer.stop(c)
    after
      exec!(admin, "DROP TABLE rr")
      GenServer.stop(admin)
    end
  end

  test "R49b: a role or path change recompiles against the namesake's column types", %{opts: opts} do
    {:ok, admin} = Adapter.connect(opts)

    for sql <- [
          "CREATE SCHEMA ns_a",
          "CREATE SCHEMA ns_b",
          "CREATE TABLE ns_a.ns (id int PRIMARY KEY, org varchar(5) NOT NULL, k text)",
          "CREATE TABLE ns_b.ns (id int PRIMARY KEY, org varchar(5) NOT NULL, k ext.citext)",
          "INSERT INTO ns_a.ns VALUES (1, 't1', 'MiX')",
          "INSERT INTO ns_b.ns VALUES (1, 't1', 'MiX')"
        ],
        do: exec!(admin, sql)

    try do
      columns = %{id: %{type: :integer}, org: %{type: :string}, k: %{type: :string}}

      ns = fn schema ->
        %{
          name: "ns",
          source: rel("ns", columns, %{source_schema: schema}),
          schemas: %{},
          joins: %{}
        }
      end

      {:ok, c} = Adapter.connect(opts)

      read = fn schema ->
        c
        |> base(ns.(schema))
        |> Selecto.select(["id"])
        |> Selecto.filter({"k", "mix"})
        |> run()
      end

      # One selecto compiled per read: each answers by its relation's type.
      assert read.("ns_a") == {:ok, []}
      assert read.("ns_b") == {:ok, [[1]]}

      # The same configured query on a connection whose path resolves the
      # namesake: resolved and compiled again, never a prefix patched in.
      plain = %{name: "ns", source: rel("ns", columns), schemas: %{}, joins: %{}}

      on_path = fn path ->
        exec!(
          admin,
          ~s|ALTER ROLE CURRENT_USER IN DATABASE "#{opts[:database]}" SET search_path = #{path}|
        )

        {:ok, connection} = Adapter.connect(opts)

        result =
          connection
          |> base(plain)
          |> Selecto.select(["id"])
          |> Selecto.filter({"k", "mix"})
          |> run()

        GenServer.stop(connection)
        result
      end

      assert on_path.("ns_a, ext") == {:ok, []}
      assert on_path.("ns_b, ext") == {:ok, [[1]]}

      # And within one connection: a host SET ROLE or search_path change
      # never redirects a read of a domain that declares its schema.
      exec!(c, "SET search_path = ns_a, ext")
      assert read.("ns_b") == {:ok, [[1]]}
      exec!(c, "SET search_path = ns_b, ext")
      assert read.("ns_a") == {:ok, []}
      GenServer.stop(c)
    after
      exec!(admin, ~s|ALTER ROLE CURRENT_USER IN DATABASE "#{opts[:database]}" RESET search_path|)
      exec!(admin, "DROP SCHEMA ns_a CASCADE")
      exec!(admin, "DROP SCHEMA ns_b CASCADE")
      GenServer.stop(admin)
    end
  end

  test "uuid and domain-over-uuid tenants: text binds, no uuid refuses, typed", %{opts: opts} do
    u1 = "0000000a-0000-0000-0000-000000000001"
    {:ok, admin} = Adapter.connect(opts)

    for sql <- [
          "CREATE DOMAIN udom AS uuid",
          "CREATE TABLE ut (id int PRIMARY KEY, org uuid NOT NULL, d udom, note text)"
        ],
        do: exec!(admin, sql)

    try do
      {:ok, c} = Adapter.connect(opts)

      insert = fn value ->
        {:ok, command} =
          Command.new(%{
            operation: :insert,
            relation: "ut",
            assignments: [
              %{field: "id", value: {:literal, 1}},
              %{field: "org", value: {:literal, value}},
              %{field: "d", value: {:literal, value}}
            ],
            metadata: %{tenant_fields: ["org"]}
          })

        Adapter.execute_write_unsafe(c, command, [])
      end

      for bad <- ["not-a-uuid", String.upcase(u1)] do
        assert {:error, %{details: %{reason: :invalid_uuid}}} = insert.(bad)
      end

      assert {:ok, %{affected_rows: 1}} = insert.(u1)

      columns = %{
        id: %{type: :integer},
        org: %{type: :string},
        d: %{type: :string},
        note: %{type: :string}
      }

      domain = %{name: "ut", source: rel("ut", columns), schemas: %{}, joins: %{}}

      read = fn tenant ->
        domain
        |> Selecto.configure(c, adapter: Adapter, validate: false, rollup_sort_fix: false)
        |> Selecto.with_tenant(tenant)
        |> Selecto.select(["id"])
        |> Selecto.filter({"d", u1})
        |> run()
      end

      assert read.(u1) == {:ok, [[1]]}
      assert read.("0000000a-0000-0000-0000-000000000002") == {:ok, []}
      assert read.("not-a-uuid") == {:refused, :invalid_uuid}
      GenServer.stop(c)
    after
      exec!(admin, "DROP TABLE ut")
      exec!(admin, "DROP DOMAIN udom")
      GenServer.stop(admin)
    end
  end

  test "Selecto's introspection resolves nothing by name, under plants", %{opts: opts} do
    {:ok, admin} = Adapter.connect(opts)
    for sql <- plants(), do: exec!(admin, sql)

    try do
      {:ok, c} = Adapter.connect(opts)
      assert {:ok, relations} = Adapter.list_relations(c, schema: "public", include_views: true)
      assert Enum.any?(relations, &(inspect(&1) =~ "items"))
      assert {:ok, %{}} = Adapter.column_catalog(c, "items")
      GenServer.stop(c)
    after
      remove_plants(admin)
      GenServer.stop(admin)
    end
  end
end
