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

  # A relation position: after FROM, JOIN or INTO (and an optional LATERAL
  # or ONLY), a name that is not a function call or qualified.
  @position ~r/(\b(?:from|join|into)\s+(?>(?:(?:lateral|only)\b\s*)?))("(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)(?!\s*[.(])(?![A-Za-z0-9_$])/i

  @host_sql "SELECT CAST(pg_catalog.current_schemas(true) AS pg_catalog.text[]) " <>
              "AS selecto_guard_host_schemas"

  @reset_sql "SELECT pg_catalog.set_config('search_path', (SELECT s.reset_val " <>
               "FROM pg_catalog.pg_settings s WHERE s.name OPERATOR(pg_catalog.=) 'search_path'), true) " <>
               "AS selecto_guard_reset_path"

  # Per name: the schema and relation the reset path (now in effect,
  # current_schemas(false), the temp schema excluded) resolves it to, and
  # the ones the host's path ($2) resolves it to.
  @resolve_sql """
  WITH selecto_reset AS (
    SELECT u.nsp, u.ord
    FROM pg_catalog.unnest(pg_catalog.current_schemas(false)) WITH ORDINALITY AS u(nsp, ord)
    JOIN pg_catalog.pg_namespace n ON n.nspname OPERATOR(pg_catalog.=) u.nsp
    WHERE n.oid OPERATOR(pg_catalog.<>) pg_catalog.pg_my_temp_schema()
  ),
  selecto_host AS (
    SELECT u.nsp, u.ord
    FROM pg_catalog.unnest($2::pg_catalog.text[]) WITH ORDINALITY AS u(nsp, ord)
  ),
  selecto_match AS (
    SELECT r.name, n.nspname, c.relname,
           n.oid OPERATOR(pg_catalog.=) pg_catalog.pg_my_temp_schema() AS is_temp,
           (CAST(c.relname AS pg_catalog.text) OPERATOR(pg_catalog.=) r.name) AS exact
    FROM pg_catalog.unnest($1::pg_catalog.text[]) AS r(name)
    JOIN pg_catalog.pg_class c
      ON CAST(c.relname AS pg_catalog.text) OPERATOR(pg_catalog.=) r.name
         OR CAST(c.relname AS pg_catalog.text) OPERATOR(pg_catalog.=) pg_catalog.lower(r.name)
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
           AS selecto_guard_relation_host_temp
  FROM pg_catalog.unnest($1::pg_catalog.text[]) AS r(name)
  """

  @doc """
  `{:ok, %{name => ~s("schema"."relation")}}` for every name (declared
  qualified names map to their own quoted form), or a typed refusal. Runs
  its statements through `execute` inside a savepoint (`in_transaction?`)
  or a transaction of its own, rolled back afterwards.
  """
  def resolve(execute, names, in_transaction? \\ true)

  def resolve(_execute, [], _in_transaction?), do: {:ok, %{}}

  def resolve(execute, names, in_transaction?) do
    names = names |> Enum.map(&to_string/1) |> Enum.uniq()
    {qualified, plain} = Enum.split_with(names, &String.contains?(&1, "."))
    own = Map.new(qualified, &{&1, quote_relation(&1)})

    if plain == [] do
      {:ok, own}
    else
      case lookup(execute, plain, in_transaction?) do
        {:ok, rows} -> decide(rows, own)
        :error -> unavailable()
      end
    end
  end

  defp lookup(execute, plain, in_transaction?) do
    name = "selecto_path_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

    {open, close} =
      if in_transaction?,
        do:
          {"SAVEPOINT " <> name, ["ROLLBACK TO SAVEPOINT " <> name, "RELEASE SAVEPOINT " <> name]},
        else: {"BEGIN", ["ROLLBACK"]}

    case execute.(open, []) do
      {:ok, _} ->
        result =
          with {:ok, %{rows: [[host]]}} when is_list(host) <- execute.(@host_sql, []),
               {:ok, %{rows: [[_reset]]}} <- execute.(@reset_sql, []),
               {:ok, %{rows: rows}} when is_list(rows) <- execute.(@resolve_sql, [plain, host]) do
            {:ok, rows}
          else
            _ -> :error
          end

        Enum.each(close, &execute.(&1, []))
        result

      _ ->
        :error
    end
  end

  defp decide(rows, own) do
    Enum.reduce_while(rows, {:ok, own}, fn
      [name, schema, relation, host, host_temp], {:ok, acc} ->
        cond do
          is_nil(schema) and host_temp ->
            {:halt,
             refusal(
               :relation_unresolved,
               "a domain relation resolves only to a temporary table; declare it as pg_temp.<name>",
               [name]
             )}

          is_nil(schema) and is_nil(host) ->
            {:halt, unresolved([name])}

          # R38a: a temp namesake of a relation qualified to its resolved
          # (non-temp) schema cannot affect it: not refused.
          is_nil(schema) or (host != nil and host != schema and not host_temp) ->
            {:halt,
             refusal(
               :relation_ambiguous,
               "PostgreSQL resolves a domain relation differently under the session's search_path " <>
                 "than under its reset search_path; declare the schema on the domain",
               [name]
             )}

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
