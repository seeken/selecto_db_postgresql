defmodule SelectoDBPostgreSQL.RelationPath do
  @moduledoc false

  # Schema-qualified relations and a pinned search_path for generated reads
  # and writes (item 58, rulings R23b, R29, R30, R36, R38). A search_path
  # switch that persists on the connection (a deferred trigger's
  # set_config(..., false) racing a host REPEATABLE READ transaction, or any
  # earlier session-level set_config) must not redirect a later Selecto
  # statement to another schema's relation, function or operator.
  #
  # A relation a domain or command declares with its schema
  # ("schema.relation", "pg_temp.relation" for a host temp table) keeps it.
  # Any other name is resolved against the connection's RESET search_path,
  # which PostgreSQL itself parses (R36): inside a savepoint or transaction
  # of its own that is rolled back afterwards, the lookup applies
  # pg_settings.reset_val with set_config(..., true) and reads
  # current_schemas(false), so quoting, case folding, "$user" and missing
  # schemas follow the server; pg_temp never takes part. The name is also
  # looked up under the host's CURRENT path (current_schemas(true), the temp
  # schema included where PostgreSQL searches it): a name the two resolve
  # to different non-temp relations refuses (`relation_ambiguous`, R38), a
  # name only the session's temp schema holds refuses unless declared, a
  # temp namesake of a resolved relation is harmless once it is qualified
  # (R38a: resolution never prefers pg_temp), and a name neither
  # reaches, or a lookup that cannot run, refuses (`relation_unresolved`):
  # no unqualified statement runs. Every function and operator of the
  # lookup is pg_catalog qualified, and its columns carry the
  # `selecto_guard_` tag.
  #
  # `pinned_path/1` is the search_path every Selecto statement runs under
  # (R30): pg_catalog first, the schemas of its relations, pg_temp last.

  alias Selecto.Write.Error
  alias SelectoDBPostgreSQL.RelationPath.Scanner
  alias SelectoDBPostgreSQL.Qualify
  alias SelectoDBPostgreSQL.SessionGuard

  @path_key "selecto:reset_schemas"

  @temp_key "selecto:temp_relations"

  # One statement, so the reset path it sets ends with it (its own implicit
  # transaction, or the savepoint around it in a host transaction). In order,
  # each CTE reading the one before so nothing runs early: the host's
  # schemas; search_path set to its reset value, PostgreSQL resolving it
  # (R36); the reset path's schemas and the session's temp relations (R42
  # FINAL). Per name ($1): the schema and relation the reset path resolves it
  # to (the temp schema excluded) and the ones the host's path resolves it
  # to. No name still gives one row: the schemas are needed whatever the
  # names.
  @lookup_sql """
  WITH selecto_before AS MATERIALIZED (
    SELECT CAST(pg_catalog.current_schemas(true) AS pg_catalog.text[]) AS schemas
  ),
  selecto_path AS MATERIALIZED (
    SELECT b.schemas,
           pg_catalog.set_config('search_path', (SELECT s.reset_val FROM pg_catalog.pg_settings s
             WHERE s.name OPERATOR(pg_catalog.=) 'search_path'), true) AS reset_path
    FROM selecto_before b
  ),
  selecto_schemas AS MATERIALIZED (
    SELECT p.schemas AS host, p.reset_path,
           CAST(pg_catalog.current_schemas(false) AS pg_catalog.text[]) AS reset,
           (SELECT pg_catalog.count(*) FROM pg_catalog.pg_class c
            WHERE c.relnamespace OPERATOR(pg_catalog.=) pg_catalog.pg_my_temp_schema()) AS temps
    FROM selecto_path p
  ),
  selecto_reset AS (
    SELECT u.nsp, u.ord
    FROM selecto_schemas s
    CROSS JOIN LATERAL pg_catalog.unnest(s.reset) WITH ORDINALITY AS u(nsp, ord)
    JOIN pg_catalog.pg_namespace n ON n.nspname OPERATOR(pg_catalog.=) u.nsp
    WHERE n.oid OPERATOR(pg_catalog.<>) pg_catalog.pg_my_temp_schema()
  ),
  selecto_host AS (
    SELECT u.nsp, u.ord
    FROM selecto_schemas s
    CROSS JOIN LATERAL pg_catalog.unnest(s.host) WITH ORDINALITY AS u(nsp, ord)
  ),
  selecto_match AS (
    SELECT r.name, n.nspname, c.relname,
           n.oid OPERATOR(pg_catalog.=) pg_catalog.pg_my_temp_schema() AS is_temp,
           (CAST(c.relname AS pg_catalog.text) OPERATOR(pg_catalog.=) r.name) AS exact
    FROM pg_catalog.unnest($1::pg_catalog.text[]) AS r(name)
    CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[r.name, pg_catalog.lower(r.name)]) AS v(name)
    JOIN pg_catalog.pg_class c
      ON c.relname OPERATOR(pg_catalog.=) CAST(v.name AS pg_catalog.name)
         AND CAST(c.relname AS pg_catalog.text) OPERATOR(pg_catalog.=) v.name
    JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
    WHERE c.relkind OPERATOR(pg_catalog.=) ANY (ARRAY['r', 'p', 'v', 'm', 'f']::pg_catalog."char"[])
  )
  SELECT r.name AS selecto_guard_relation_input,
         (SELECT CAST(m.nspname AS pg_catalog.text) FROM selecto_match m
          JOIN selecto_reset p ON p.nsp OPERATOR(pg_catalog.=) m.nspname
          WHERE m.name OPERATOR(pg_catalog.=) r.name ORDER BY p.ord, m.exact DESC LIMIT 1)
           AS selecto_guard_relation_schema,
         (SELECT CAST(m.relname AS pg_catalog.text) FROM selecto_match m
          JOIN selecto_reset p ON p.nsp OPERATOR(pg_catalog.=) m.nspname
          WHERE m.name OPERATOR(pg_catalog.=) r.name ORDER BY p.ord, m.exact DESC LIMIT 1)
           AS selecto_guard_relation_name,
         (SELECT CAST(m.nspname AS pg_catalog.text) FROM selecto_match m
          JOIN selecto_host h ON h.nsp OPERATOR(pg_catalog.=) m.nspname
          WHERE m.name OPERATOR(pg_catalog.=) r.name ORDER BY h.ord, m.exact DESC LIMIT 1)
           AS selecto_guard_relation_host_schema,
         COALESCE((SELECT m.is_temp FROM selecto_match m
          JOIN selecto_host h ON h.nsp OPERATOR(pg_catalog.=) m.nspname
          WHERE m.name OPERATOR(pg_catalog.=) r.name ORDER BY h.ord, m.exact DESC LIMIT 1), false)
           AS selecto_guard_relation_host_temp,
         x.reset AS selecto_guard_reset_schemas,
         x.temps AS selecto_guard_temp_relations,
         x.reset_path AS selecto_guard_reset_path
  FROM selecto_schemas x
  LEFT JOIN pg_catalog.unnest($1::pg_catalog.text[]) AS r(name) ON true
  """

  @doc """
  `{:ok, %{name => ~s("schema"."relation")}}` for every name (declared
  qualified names map to their own quoted form), or a typed refusal. Runs
  its statements through `execute` inside a savepoint (`in_transaction?`)
  or a transaction of its own, rolled back afterwards.
  """
  def resolve(execute, names, in_transaction? \\ true)

  def resolve(execute, names, in_transaction?) do
    names = names |> Enum.map(&to_string/1) |> Enum.uniq()
    {qualified, plain} = Enum.split_with(names, &String.contains?(&1, "."))
    own = Map.new(qualified, &{&1, quote_relation(&1)})

    # The reset path's schemas are read even when every name is declared:
    # the pin needs them (R43).
    case lookup(execute, plain, in_transaction?) do
      {:ok, rows, schemas, temps} ->
        decide(rows, own |> Map.put(@path_key, schemas) |> Map.put(@temp_key, temps))

      {:failed, error} ->
        error

      :error ->
        unavailable()
    end
  end

  # R46 FINAL-2: per column of each resolved relation, the schema whose
  # operators compare it and the operators' argument type: a domain's base
  # type (domains over domains followed), text for varchar, the type itself
  # otherwise; pg_catalog's operators for built-in, enum, range, multirange
  # and array types and a user's composite, the type's own schema only for a
  # member of an extension (citext, a composite like topogeometry, whose
  # implicit casts leave PostgreSQL no unique operator; R47: pg_depend
  # deptype 'e').
  @types_sql """
  WITH RECURSIVE selecto_rel(name, qualified) AS (
    SELECT * FROM ROWS FROM (pg_catalog.unnest($1::pg_catalog.text[]), pg_catalog.unnest($2::pg_catalog.text[]))
  ),
  selecto_base(name, attname, oid, depth) AS (
    SELECT r.name, CAST(a.attname AS pg_catalog.text), a.atttypid, 0
    FROM selecto_rel r
    JOIN pg_catalog.pg_attribute a
      ON a.attrelid OPERATOR(pg_catalog.=) pg_catalog.to_regclass(r.qualified)
     AND a.attnum OPERATOR(pg_catalog.>) 0 AND NOT a.attisdropped
    UNION ALL
    SELECT b.name, b.attname, t.typbasetype, b.depth OPERATOR(pg_catalog.+) 1
    FROM selecto_base b JOIN pg_catalog.pg_type t ON t.oid OPERATOR(pg_catalog.=) b.oid
    WHERE t.typtype OPERATOR(pg_catalog.=) 'd' AND b.depth OPERATOR(pg_catalog.<) 16
  )
  SELECT DISTINCT ON (b.name, b.attname)
         b.name AS selecto_guard_type_relation,
         b.attname AS selecto_guard_type_column,
         CASE WHEN n.oid OPERATOR(pg_catalog.=) CAST(11 AS pg_catalog.oid)
                OR t.typtype OPERATOR(pg_catalog.=) ANY (ARRAY['e', 'r', 'm']::pg_catalog."char"[])
                OR t.typcategory OPERATOR(pg_catalog.=) 'A'
                OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                  WHERE d.classid OPERATOR(pg_catalog.=) CAST('pg_catalog.pg_type' AS pg_catalog.regclass)
                    AND d.objid OPERATOR(pg_catalog.=) t.oid AND d.deptype OPERATOR(pg_catalog.=) 'e')
              THEN 'pg_catalog' ELSE pg_catalog.quote_ident(CAST(n.nspname AS pg_catalog.text)) END
           AS selecto_guard_type_schema,
         CASE WHEN t.oid OPERATOR(pg_catalog.=) CAST(1043 AS pg_catalog.oid) THEN 'pg_catalog.text'
              ELSE pg_catalog.quote_ident(CAST(n.nspname AS pg_catalog.text)) OPERATOR(pg_catalog.||) '.'
                     OPERATOR(pg_catalog.||) pg_catalog.quote_ident(CAST(t.typname AS pg_catalog.text)) END
           AS selecto_guard_type_arg
  FROM selecto_base b
  JOIN pg_catalog.pg_type t ON t.oid OPERATOR(pg_catalog.=) b.oid
  JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) t.typnamespace
  WHERE t.typtype OPERATOR(pg_catalog.<>) 'd'
  ORDER BY b.name, b.attname, b.depth DESC
  """

  @doc """
  `{:ok, %{relation => %{column => {schema, arg}}}}` for a resolve/3 map's
  relations, keyed like the map.
  """
  @missing_sql "SELECT r.n FROM ROWS FROM (pg_catalog.unnest($1::pg_catalog.text[]), " <>
                 "pg_catalog.unnest($2::pg_catalog.text[])) AS r(n, q) " <>
                 "WHERE pg_catalog.to_regclass(r.q) IS NULL"

  @doc false
  # Of `{name, quoted}` pairs, the names no relation answers to: refused as
  # unresolved.
  def missing(execute, pairs) do
    case execute.(@missing_sql, [Enum.map(pairs, &elem(&1, 0)), Enum.map(pairs, &elem(&1, 1))]) do
      {:ok, %{rows: []}} ->
        :ok

      {:ok, %{rows: rows}} when is_list(rows) ->
        refusal(
          :relation_unresolved,
          "a relation its declared schema names does not exist; check source_schema " <>
            "on the domain that names it, or the root it takes it from",
          List.flatten(rows)
        )

      other ->
        if SessionGuard.standing(other), do: other, else: unavailable()
    end
  end

  def operator_types(execute, relations) do
    pairs =
      for {name, qualified} <- relations,
          is_binary(qualified),
          name not in [@path_key, @temp_key],
          do: {name, qualified}

    if pairs == [] do
      {:ok, %{}}
    else
      case execute.(@types_sql, [Enum.map(pairs, &elem(&1, 0)), Enum.map(pairs, &elem(&1, 1))]) do
        {:ok, %{rows: rows}} when is_list(rows) ->
          # A column of an extension member type is left out: its operator
          # depends on the operands, and only the qualifier, with the
          # type's member operators in hand, may name one (H1).
          {:ok,
           rows
           |> Enum.filter(fn [_name, _column, schema, _arg] -> schema == "pg_catalog" end)
           |> Enum.reduce(%{}, fn [name, column, schema, arg], acc ->
             Map.update(
               acc,
               name,
               %{column => {schema, arg}},
               &Map.put(&1, column, {schema, arg})
             )
           end)}

        other ->
          if SessionGuard.standing(other), do: other, else: unavailable()
      end
    end
  end

  @doc false
  # The key under which resolve/3 records the reset path's schemas (no SQL
  # identifier can contain it unquoted, and generated SQL never quotes it).
  def path_key, do: @path_key

  defp ambiguous(names) do
    refusal(
      :relation_ambiguous,
      "resolves a domain relation differently under the session's search_path " <>
        "than under its reset search_path; declare the schema on the domain",
      names
    )
  end

  @doc """
  The schema-qualified relations `sql` names, each as written, in order: what
  a read of it holds (R69). `qualify/2` has already named them.
  """
  def qualified_refs(sql) do
    sql = IO.iodata_to_binary(sql)

    sql
    |> Scanner.refs()
    |> Enum.filter(& &1.qualified?)
    |> Enum.map(&binary_part(sql, &1.start, &1.length))
    |> Enum.uniq()
  end

  defp lookup(execute, plain, true) do
    name = "selecto_path_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

    case execute.(SessionGuard.control("SAVEPOINT " <> name), []) do
      {:ok, _} ->
        result = lookup(execute, plain, false)

        case SessionGuard.undo_savepoint(execute, name) do
          :ok -> result
          failure -> if match?({:ok, _, _, _}, result), do: lookup_failure(failure), else: result
        end

      other ->
        lookup_failure(other)
    end
  end

  defp lookup(execute, plain, false) do
    case execute.(@lookup_sql, [plain]) do
      {:ok, %{rows: [[_, _, _, _, _, schemas, temps, _] | _] = rows}} when is_list(schemas) ->
        rows =
          for [name, schema, relation, host, temp | _] <- rows,
              not is_nil(name),
              do: [name, schema, relation, host, temp]

        {:ok, rows, Enum.reject(schemas, &String.starts_with?(&1, "pg_temp")), temps}

      other ->
        lookup_failure(other)
    end
  end

  # A cancel or a lost connection is not an unresolved relation.
  defp lookup_failure(result),
    do: if(SessionGuard.standing(result), do: {:failed, result}, else: :error)

  defp decide(rows, own) do
    Enum.reduce_while(rows, {:ok, own}, fn
      [name, schema, relation, host, host_temp], {:ok, acc} ->
        cond do
          is_nil(schema) and host_temp ->
            {:halt,
             refusal(
               :relation_unresolved,
               "a domain relation resolves only to a temporary table; declare " <>
                 "source_schema: \"pg_temp\" on the domain that names it. An ad-hoc " <>
                 "join's table takes its root's schema, so it reaches a temporary " <>
                 "table only under a root declaring pg_temp; otherwise use " <>
                 "join_subquery on a domain declaring pg_temp",
               [name]
             )}

          is_nil(schema) and is_nil(host) ->
            {:halt, unresolved([name])}

          # R38a: a temp namesake of a relation qualified to its resolved
          # (non-temp) schema cannot affect it: not refused.
          is_nil(schema) or (host != nil and host != schema and not host_temp) ->
            {:halt, ambiguous([name])}

          true ->
            {:cont,
             {:ok,
              Map.put(acc, name, quote_identifier(schema) <> "." <> quote_identifier(relation))}}
        end

      _row, _acc ->
        {:halt, unavailable()}
    end)
  end

  @doc """
  The search_path a statement over `relations` (a resolve/3 map) runs under
  (R43): pg_catalog, the declared schemas of the relations, every schema
  of the reset search_path (as the server parsed it), pg_temp last. Never
  only the statement's own relation schemas: an extension's types,
  functions and operators (citext in `ext`) must resolve as the host's own
  configuration resolves them.
  """
  def pinned_path(relations) when is_map(relations) do
    reset = Map.get(relations, @path_key, [])

    declared =
      relations
      |> Map.drop([@path_key, @temp_key])
      |> Map.values()
      |> Enum.filter(&is_binary/1)
      |> Enum.sort()
      |> Enum.map(&schema_of/1)

    # R43: declared schemas first, then the reset path's.
    schemas =
      (declared ++ reset)
      |> Enum.reject(&(&1 in [nil, "pg_catalog"] or String.starts_with?(&1, "pg_temp")))
      |> Enum.uniq()

    Enum.join(["pg_catalog" | Enum.map(schemas, &quote_identifier/1)] ++ ["pg_temp"], ", ")
  end

  def pinned_path(_relations), do: "pg_catalog, pg_temp"

  @doc """
  The relation names `qualify/2` would rewrite in `sql` (FROM, JOIN and INTO
  positions, not CTE names or function calls).
  """
  def names(sql) do
    sql = IO.iodata_to_binary(sql)
    ctes = cte_names(sql)

    sql
    |> Scanner.refs()
    |> Enum.reject(&(&1.qualified? or &1.folded in ctes))
    |> Enum.map(& &1.name)
    |> Enum.uniq()
  end

  @doc """
  `:ok` when every relation position (FROM, JOIN, INTO) of `sql` is
  schema-qualified, a function call, a derived table or one of the
  statement's CTE names; otherwise a typed refusal. Checked on the exact
  text a Selecto statement executes (R29).
  """
  def qualified(sql) do
    case names(sql) do
      [] -> :ok
      missing -> unresolved(missing)
    end
  end

  defp schema_of(quoted) do
    case Regex.run(~r/^"((?:[^"]|"")*)"\./, quoted, capture: :all_but_first) do
      [schema] -> String.replace(schema, "\"\"", "\"")
      _ -> nil
    end
  end

  defp unresolved(missing),
    do:
      refusal(
        :relation_unresolved,
        "could not resolve a relation's schema from the reset search_path; " <>
          "declare the schema on the domain",
        missing
      )

  defp refusal(reason, message, relations),
    do:
      {:error,
       Error.new(:query_enforcement, "PostgreSQL " <> message,
         details: %{
           code: :query_rule_not_evaluable,
           reason: reason,
           relations: relations,
           adapter: :postgresql
         }
       )}

  defp unavailable,
    do:
      {:error,
       Error.new(:query_enforcement, "PostgreSQL cannot resolve the schemas of the relations",
         details: %{
           code: :query_rule_not_evaluable,
           reason: :relation_unresolved,
           adapter: :postgresql
         }
       )}

  @doc """
  Rewrites the relation names a generated statement names after FROM, JOIN
  or INTO (quoted or bare, not already qualified, not a function call) to
  their qualified form, `{:ok, sql}`.

  CTE names are scoped (R33): in a leading non-recursive WITH, a CTE's own
  body and the bodies before it see the real relation of that name (a
  query member named like the relation it reads), the bodies after it and
  the main query see the CTE. A CTE name equal to a relation of `map` that
  this cannot scope (WITH RECURSIVE, a nested WITH) refuses.
  """
  def qualify(sql, map) do
    sql = IO.iodata_to_binary(sql)

    cond do
      scan_ctes(sql) == :unsure -> ctes_unresolved()
      map_size(map) == 0 -> {:ok, sql}
      true -> qualify_sure(sql, map)
    end
  end

  defp qualify_sure(sql, map) do
    case leading_with(sql) do
      {:ok, recursive?, ctes, main} when not recursive? ->
        names = Enum.map(ctes, &elem(&1, 0))
        nested = cte_names(main) ++ Enum.flat_map(ctes, &cte_names(elem(&1, 2)))

        with :ok <- no_collision(nested, map) do
          {bodies, _seen} =
            Enum.map_reduce(ctes, [], fn {name, head, body}, seen ->
              {head <> rewrite(body, map, seen ++ cte_names(body)) <> ")", seen ++ [name]}
            end)

          {:ok,
           IO.iodata_to_binary([
             bodies_prefix(sql),
             Enum.join(bodies, ","),
             rewrite(main, map, names ++ cte_names(main))
           ])}
        end

      _other ->
        ctes = cte_names(sql)

        with :ok <- no_collision(ctes, map) do
          {:ok, rewrite(sql, map, ctes)}
        end
    end
  end

  defp rewrite(sql, map, ctes) do
    sql = IO.iodata_to_binary(sql)

    sql
    |> Scanner.refs()
    |> Enum.reject(&(&1.qualified? or &1.folded in ctes))
    |> Enum.filter(&Map.has_key?(map, &1.name))
    |> Enum.reverse()
    |> Enum.reduce(sql, fn ref, acc ->
      binary_part(acc, 0, ref.start) <>
        Map.fetch!(map, ref.name) <>
        binary_part(acc, ref.start + ref.length, byte_size(acc) - ref.start - ref.length)
    end)
  end

  defp no_collision(ctes, map) do
    relations = map |> Map.keys() |> Enum.map(&String.downcase/1)

    case Enum.filter(ctes, &(String.downcase(&1) in relations)) do
      [] ->
        :ok

      names ->
        {:error,
         Error.new(
           :query_enforcement,
           "PostgreSQL cannot scope a CTE named like a relation the statement reads",
           details: %{
             code: :query_rule_not_evaluable,
             reason: :cte_name_collision,
             names: names,
             adapter: :postgresql
           }
         )}
    end
  end

  @with_head ~r/\A(\s*with(\s+recursive)?\s+)/i
  @cte_head ~r/\A(\s*("(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)\s*(?:\([^)]*\)\s*)?as\s*(?:not\s+)?(?:materialized\s*)?\()/i

  # A leading WITH: {:ok, recursive?, [{name, head, body}], main}, the
  # bodies without their closing parenthesis; :none otherwise.
  defp leading_with(sql) do
    case Regex.run(@with_head, sql, capture: :all_but_first) do
      [head | recursive] ->
        rest = binary_part(sql, byte_size(head), byte_size(sql) - byte_size(head))

        case parse_ctes(rest, []) do
          {:ok, ctes, main} ->
            # SEARCH or CYCLE after a body: the general path reads the list.
            if Regex.match?(
                 ~r/\A\s*(?:\(|select\b|insert\b|update\b|delete\b|merge\b|values\b|table\b|with\b)/i,
                 main
               ),
               do: {:ok, recursive not in [[], [""]], ctes, main},
               else: :none

          :error ->
            :none
        end

      nil ->
        :none
    end
  end

  defp bodies_prefix(sql), do: @with_head |> Regex.run(sql) |> hd()

  defp parse_ctes(rest, acc) do
    with [head, name] <- Regex.run(@cte_head, rest, capture: :all_but_first),
         after_head = binary_part(rest, byte_size(head), byte_size(rest) - byte_size(head)),
         {:ok, body, tail} <- split_paren(after_head) do
      acc = [{fold_name(name), head, body} | acc]

      case Regex.run(~r/\A\s*,/, tail) do
        [comma] ->
          parse_ctes(binary_part(tail, byte_size(comma), byte_size(tail) - byte_size(comma)), acc)

        nil ->
          {:ok, Enum.reverse(acc), tail}
      end
    else
      _ -> :error
    end
  end

  # The text up to the parenthesis closing an already opened one, skipping
  # string literals and quoted identifiers.
  defp split_paren(text), do: split_paren(text, 0, 1)

  defp split_paren(text, index, depth) when index < byte_size(text) do
    case :binary.at(text, index) do
      ?( ->
        split_paren(text, index + 1, depth + 1)

      ?) when depth == 1 ->
        {:ok, binary_part(text, 0, index),
         binary_part(text, index + 1, byte_size(text) - index - 1)}

      ?) ->
        split_paren(text, index + 1, depth - 1)

      quote when quote in [?', ?"] ->
        case :binary.match(text, <<quote>>, scope: {index + 1, byte_size(text) - index - 1}) do
          {close, 1} -> split_paren(text, close + 1, depth)
          :nomatch -> :error
        end

      _ ->
        split_paren(text, index + 1, depth)
    end
  end

  defp split_paren(_text, _index, _depth), do: :error

  # H1: read from tokens, so a comma with no space, a comment, a U& name or a
  # quoted column list is a CTE like any other. Names as PostgreSQL folds them.
  defp cte_names(sql) do
    case scan_ctes(sql) do
      {:ok, names} -> names
      :unsure -> []
    end
  end

  defp scan_ctes(sql) do
    sql |> IO.iodata_to_binary() |> Qualify.tokenize() |> walk_ctes(nil, [])
  catch
    {:qualify_refuse, _reason, _message} -> :unsure
  end

  defp walk_ctes([], _previous, names), do: {:ok, names}

  defp walk_ctes([{:id, word} = token | rest], _previous, names) do
    if String.upcase(word) == "WITH" and not with_option?(rest) do
      list = drop_recursive(rest)

      case cte_list(list, []) do
        {:ok, found} -> walk_ctes(list, token, names ++ found)
        :error -> :unsure
      end
    else
      walk_ctes(rest, token, names)
    end
  end

  defp walk_ctes([token | rest], _previous, names), do: walk_ctes(rest, token, names)

  defp drop_recursive([{:id, word} | rest] = tokens) do
    if String.upcase(word) == "RECURSIVE", do: rest, else: tokens
  end

  defp drop_recursive(tokens), do: tokens

  defp cte_list([{kind, _} = name | rest], names) when kind in [:id, :qid] do
    with {:ok, rest} <- skip_columns(rest),
         {:ok, rest} <- word(rest, "AS"),
         rest = optional_materialized(rest),
         [{:punct, "("} | body] <- rest,
         {:ok, rest} <- skip_group(body, 1) do
      names = [Qualify.folded_name(name) | names]

      with {:ok, rest} <- skip_search(rest),
           {:ok, rest} <- skip_cycle(rest) do
        case rest do
          [{:punct, ","} | more] -> cte_list(more, names)
          _other -> if statement_start?(rest), do: {:ok, names}, else: :error
        end
      end
    else
      _other -> :error
    end
  end

  defp cte_list(_tokens, _names), do: :error

  # WITH ORDINALITY, WITH TIME ZONE and the like are no WITH clause.
  defp with_option?([{:id, word} | _]) do
    String.upcase(word) in ~w(ORDINALITY TIME TIES DATA NO CHECK LOCAL CASCADED UNIQUE
                              WRAPPER CONDITIONAL UNCONDITIONAL KEYS ORDINALITY)
  end

  defp with_option?(_tokens), do: false

  defp statement_start?([{:punct, "("} | _]), do: true

  defp statement_start?([{:id, word} | _]),
    do: String.upcase(word) in ~w(SELECT INSERT UPDATE DELETE MERGE VALUES TABLE WITH)

  defp statement_start?(_tokens), do: false

  # SEARCH { DEPTH | BREADTH } FIRST BY col [, ...] SET col
  defp skip_search(tokens) do
    with {:ok, rest} <- word(tokens, "SEARCH") do
      with [{:id, _order} | rest] <- rest,
           {:ok, rest} <- word(rest, "FIRST"),
           {:ok, rest} <- word(rest, "BY"),
           {:ok, rest} <- skip_names(rest),
           {:ok, rest} <- word(rest, "SET"),
           [{kind, _} | rest] when kind in [:id, :qid] <- rest do
        {:ok, rest}
      else
        _other -> :error
      end
    else
      :error -> {:ok, tokens}
    end
  end

  # CYCLE col [, ...] SET col [TO value DEFAULT value] USING col
  defp skip_cycle(tokens) do
    with {:ok, rest} <- word(tokens, "CYCLE") do
      with {:ok, rest} <- skip_names(rest),
           {:ok, rest} <- word(rest, "SET"),
           [{kind, _} | rest] when kind in [:id, :qid] <- rest,
           {:ok, rest} <- skip_to_using(rest),
           [{kind2, _} | rest] when kind2 in [:id, :qid] <- rest do
        {:ok, rest}
      else
        _other -> :error
      end
    else
      :error -> {:ok, tokens}
    end
  end

  defp skip_names([{kind, _}, {:punct, ","} | rest]) when kind in [:id, :qid],
    do: skip_names(rest)

  defp skip_names([{kind, _} | rest]) when kind in [:id, :qid], do: {:ok, rest}
  defp skip_names(_tokens), do: :error

  defp skip_to_using([]), do: :error

  defp skip_to_using([_token | rest] = tokens) do
    case word(tokens, "USING") do
      {:ok, after_using} -> {:ok, after_using}
      :error -> skip_to_using(rest)
    end
  end

  defp skip_columns([{:punct, "("} | rest]), do: skip_group(rest, 1)
  defp skip_columns(tokens), do: {:ok, tokens}

  defp word([{:id, text} | rest], expected) do
    if String.upcase(text) == expected, do: {:ok, rest}, else: :error
  end

  defp word(_tokens, _expected), do: :error

  defp optional_materialized(tokens) do
    tokens =
      case word(tokens, "NOT") do
        {:ok, rest} -> rest
        :error -> tokens
      end

    case word(tokens, "MATERIALIZED") do
      {:ok, rest} -> rest
      :error -> tokens
    end
  end

  defp skip_group(tokens, 0), do: {:ok, tokens}
  defp skip_group([], _depth), do: :error
  defp skip_group([{:punct, "("} | rest], depth), do: skip_group(rest, depth + 1)
  defp skip_group([{:punct, ")"} | rest], depth), do: skip_group(rest, depth - 1)
  defp skip_group([_token | rest], depth), do: skip_group(rest, depth)

  defp ctes_unresolved,
    do:
      refusal(
        :relation_unresolved,
        "cannot read a statement's WITH list; its CTE names must not be taken for relations",
        []
      )

  defp fold_name("\"" <> _ = name), do: unquote_name(name)
  defp fold_name(name), do: String.downcase(name)

  defp unquote_name("\"" <> _ = name),
    do: name |> String.slice(1..-2//1) |> String.replace("\"\"", "\"")

  defp unquote_name(name), do: name

  defp quote_relation(relation),
    do: relation |> String.split(".") |> Enum.map_join(".", &quote_identifier/1)

  defp quote_identifier(identifier),
    do: "\"" <> String.replace(to_string(identifier), "\"", "\"\"") <> "\""
end
