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
  alias SelectoDBPostgreSQL.Adapter

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
    {"citext aggregate in HAVING",
     "SELECT id FROM items WHERE org = 't1' GROUP BY id HAVING max(ci) = 'MIXED' ORDER BY id"},
    {"citext overload", "SELECT replace(ci, 'M', 'z') FROM items WHERE id = 3"},
    {"citext regexp overload", "SELECT regexp_replace(ci, 'MIX', 'z') FROM items WHERE id = 3"},
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
    {"text against citext", "SELECT id FROM items WHERE name = ci ORDER BY id"}
  ]

  test "hand-built reads answer as natively, under plants", %{opts: opts} do
    {:ok, setup} = Adapter.connect(opts)
    exec!(setup, "CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA ext")
    GenServer.stop(setup)

    {:ok, c} = Adapter.connect(opts)
    native = for {label, sql} <- @reads, into: %{}, do: {label, exec!(c, sql).rows}
    GenServer.stop(c)

    {:ok, admin} = Adapter.connect(opts)
    for sql <- plants(), do: exec!(admin, sql)

    try do
      {:ok, c} = Adapter.connect(opts)

      different =
        for {label, sql} <- @reads,
            result = Adapter.execute(c, sql, [], selecto_read: true),
            not match?({:ok, %{rows: rows}} when rows == :erlang.map_get(label, native), result),
            do: {label, native[label], summary(result)}

      GenServer.stop(c)
      assert different == []
    after
      remove_plants(admin)
      GenServer.stop(admin)
    end
  end

  defp summary({:ok, %{rows: rows}}), do: rows
  defp summary({:error, %{details: details} = error}), do: {details[:reason], error.message}
  defp summary(other), do: other

  test "an expression that may be citext refuses against an untyped value (R49c)", %{opts: opts} do
    {:ok, c} = Adapter.connect(opts)

    for sql <- [
          "SELECT id FROM items WHERE COALESCE(ci, 'x') = 'MIXED'",
          "SELECT id FROM items WHERE (CASE WHEN n > 0 THEN ci END) = 'MIXED'",
          "SELECT id FROM items WHERE ci = ANY($1)"
        ] do
      params = if String.contains?(sql, "$1"), do: [["MIXED"]], else: []

      assert {:error, %{details: %{reason: :operand_type_unknown}}} =
               Adapter.execute(c, sql, params, selecto_read: true),
             sql
    end

    # A computed field over citext through the domain: typed or refused,
    # never pg_catalog's text comparison.
    computed =
      domain()
      |> put_in([:source, :columns, :ci_or], %{
        type: :string,
        computed: %{kind: :expression, expression: ["coalesce", ["field", "ci"], ["field", "vc"]]}
      })
      |> update_in([:source, :fields], &[:ci_or | &1])

    assert {:refused, :operand_type_unknown} =
             c
             |> base(computed)
             |> Selecto.select(["id"])
             |> Selecto.filter({"ci_or", "MIXED"})
             |> run()

    GenServer.stop(c)
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

      read = &Adapter.execute(&1, &2, [], selecto_read: true)

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
