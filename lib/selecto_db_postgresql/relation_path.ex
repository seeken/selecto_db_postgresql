defmodule SelectoDBPostgreSQL.RelationPath do
  @moduledoc false

  # Schema-qualified relations and a pinned search_path for generated reads
  # and writes (item 58, rulings R23b, R29, R30). A search_path switch that
  # persists on the connection (a deferred trigger's set_config(..., false)
  # racing a host REPEATABLE READ transaction, or any earlier session-level
  # set_config) must not redirect a later Selecto statement to another
  # schema's relation, function or operator of the same name.
  #
  # A relation name a domain declares with its schema ("schema.relation")
  # keeps it. Any other name is resolved against the connection's RESET
  # search_path only (pg_settings.reset_val: the server, database, role or
  # startup value, which SET and set_config(..., false) do not change), with
  # "$user" as current_user (as PostgreSQL expands it), schemas that do
  # not exist and pg_temp skipped. A name the reset path does not reach but the session's own
  # temp schema holds is the host's temp relation (item 19: the shadow check
  # refuses a temp relation that has a namesake) and is named
  # "pg_temp_N"."name" explicitly. Never the current search_path: a host
  # that SETs search_path to reach another schema declares that schema on
  # the domain. A name neither reaches, or a lookup that cannot run (no connection,
  # an aborted transaction), refuses: no unqualified statement runs (fail
  # closed). Every function and operator of the lookup is pg_catalog
  # qualified, and its columns carry the `selecto_guard_` tag.
  #
  # `pinned_path/1` is the search_path every Selecto statement runs under
  # (R30): pg_catalog first, the schemas of its relations, pg_temp last.

  alias Selecto.Write.Error

  @position ~r/(\b(?:from|join|into)\s+)("(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)(?!\s*[.(])(?![A-Za-z0-9_$])/i

  @resolve_sql """
  WITH selecto_path AS (
    SELECT u.ord,
           CASE WHEN pg_catalog.btrim(u.part) OPERATOR(pg_catalog.=) ANY (ARRAY['"$user"', '$user'])
                THEN CAST(CURRENT_USER AS pg_catalog.text)
                ELSE pg_catalog.btrim(pg_catalog.btrim(u.part), '"') END AS nsp
    FROM pg_catalog.pg_settings s,
         pg_catalog.unnest(pg_catalog.string_to_array(s.reset_val, ',')) WITH ORDINALITY AS u(part, ord)
    WHERE s.name OPERATOR(pg_catalog.=) 'search_path'
      AND pg_catalog.btrim(u.part) OPERATOR(pg_catalog.<>) 'pg_temp'
    UNION ALL
    SELECT 1000000, CAST(n.nspname AS pg_catalog.text)
    FROM pg_catalog.pg_namespace n
    WHERE n.oid OPERATOR(pg_catalog.=) pg_catalog.pg_my_temp_schema()
  )
  SELECT DISTINCT ON (r.name)
         r.name AS selecto_guard_relation_input,
         CAST(n.nspname AS pg_catalog.text) AS selecto_guard_relation_schema,
         CAST(c.relname AS pg_catalog.text) AS selecto_guard_relation_name
  FROM pg_catalog.unnest($1::pg_catalog.text[]) AS r(name)
  JOIN pg_catalog.pg_class c
    ON CAST(c.relname AS pg_catalog.text) OPERATOR(pg_catalog.=) r.name
       OR CAST(c.relname AS pg_catalog.text) OPERATOR(pg_catalog.=) pg_catalog.lower(r.name)
  JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
  JOIN selecto_path p ON p.nsp OPERATOR(pg_catalog.=) CAST(n.nspname AS pg_catalog.text)
  WHERE c.relkind OPERATOR(pg_catalog.=) ANY (ARRAY['r', 'p', 'v', 'm', 'f']::pg_catalog."char"[])
  ORDER BY r.name, p.ord, (CAST(c.relname AS pg_catalog.text) OPERATOR(pg_catalog.=) r.name) DESC
  """

  @doc """
  `{:ok, %{name => ~s("schema"."relation")}}` for every name (declared
  qualified names map to their own quoted form), or a typed refusal when a
  name does not resolve or the lookup cannot run.
  """
  def resolve(_execute, []), do: {:ok, %{}}

  def resolve(execute, names) do
    names = names |> Enum.map(&to_string/1) |> Enum.uniq()
    {qualified, plain} = Enum.split_with(names, &String.contains?(&1, "."))
    own = Map.new(qualified, &{&1, quote_relation(&1)})

    if plain == [] do
      {:ok, own}
    else
      case execute.(@resolve_sql, [plain]) do
        {:ok, %{rows: rows}} when is_list(rows) ->
          resolved =
            Enum.reduce(rows, own, fn [name, schema, relation], acc ->
              Map.put(acc, name, quote_identifier(schema) <> "." <> quote_identifier(relation))
            end)

          case Enum.reject(plain, &Map.has_key?(resolved, &1)) do
            [] -> {:ok, resolved}
            missing -> unresolved(missing)
          end

        _other ->
          unavailable()
      end
    end
  end

  @doc """
  The search_path a statement over `relations` (a resolve/2 map) runs under:
  pg_catalog, the relations' schemas (sorted), pg_temp.
  """
  def pinned_path(relations) when is_map(relations) do
    schemas =
      relations
      |> Map.values()
      |> Enum.sort()
      |> Enum.map(&schema_of/1)
      # The session temp schema stays last, as pg_temp (temp relations are
      # named by it explicitly).
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

    @position
    |> Regex.scan(sql, capture: :all_but_first)
    |> Enum.map(fn [_keyword, name] -> unquote_name(name) end)
    |> Enum.reject(&(String.downcase(&1) in ctes))
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
      {:error,
       Error.new(
         :query_enforcement,
         "PostgreSQL could not resolve a relation's schema from the reset search_path; " <>
           "declare the schema on the domain",
         details: %{
           code: :query_rule_not_evaluable,
           reason: :relation_unresolved,
           relations: missing,
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
  def qualify(sql, map) when map_size(map) == 0, do: {:ok, IO.iodata_to_binary(sql)}

  def qualify(sql, map) do
    sql = IO.iodata_to_binary(sql)

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
    ctes = Enum.map(ctes, &String.downcase/1)

    Regex.replace(@position, sql, fn whole, keyword, name ->
      bare = unquote_name(name)

      cond do
        String.downcase(bare) in ctes -> whole
        Map.has_key?(map, bare) -> keyword <> Map.fetch!(map, bare)
        true -> whole
      end
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
          {:ok, ctes, main} -> {:ok, recursive not in [[], [""]], ctes, main}
          :error -> :none
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
      acc = [{unquote_name(name), head, body} | acc]

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

  defp cte_names(sql) do
    ~r/(?:\bwith(?:\s+recursive)?|,)\s+("(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)\s*(?:\([^)]*\)\s*)?\bas\s*(?:not\s+)?(?:materialized\s*)?\(/i
    |> Regex.scan(sql, capture: :all_but_first)
    |> Enum.map(fn [name] -> name |> unquote_name() |> String.downcase() end)
  end

  defp unquote_name("\"" <> _ = name),
    do: name |> String.slice(1..-2//1) |> String.replace("\"\"", "\"")

  defp unquote_name(name), do: name

  defp quote_relation(relation),
    do: relation |> String.split(".") |> Enum.map_join(".", &quote_identifier/1)

  defp quote_identifier(identifier),
    do: "\"" <> String.replace(to_string(identifier), "\"", "\"\"") <> "\""
end
