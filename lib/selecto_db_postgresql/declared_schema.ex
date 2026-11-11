defmodule SelectoDBPostgreSQL.DeclaredSchema do
  @moduledoc false

  # Relations a domain declares a schema for (`source_schema`, or the schema
  # its query inherits from a root that declares one, R59) are written with it
  # in the statement a read runs: Selecto compiles relation names plainly, and
  # this names the declared ones `"schema"."relation"`. Nothing is read from
  # the catalog. A relation no domain declares a schema for stays as compiled
  # and resolves by the session's search_path, as the host's own queries do.

  alias Selecto.Write.Error
  alias SelectoDBPostgreSQL.DeclaredSchema.Scanner

  @doc """
  `{:ok, %{relation => schema}}`: the schema each relation a read names is
  declared with, across every query the statement compiles
  (Selecto.Executor.read_scopes/1), each against its own declarations and
  root. `source_schema` next to a `source_table` in the query's domain decides;
  else, under a root that declares its schema, the relation is read there
  (R59). A nested query (a subquery join's, a CTE's, a LATERAL join's) whose
  domain declares no schema takes its nearest enclosing query's, and a
  retargeted query its origin's. The compiled SQL names a relation once for
  the whole statement, so a name read in two schemas, or declared in one query
  and not in another, refuses (`relation_ambiguous`).
  """
  def relations(selecto) do
    with {:ok, requests} <- requests(selecto) do
      requests
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
      |> Enum.reduce_while({:ok, %{}}, fn {table, schemas}, {:ok, acc} ->
        case Enum.uniq(schemas) do
          [nil] -> {:cont, {:ok, acc}}
          [schema] -> {:cont, {:ok, Map.put(acc, table, schema)}}
          _two -> {:halt, two_schemas([table])}
        end
      end)
    end
  end

  defp requests(selecto) do
    selecto
    |> Selecto.Executor.read_scopes()
    |> Enum.reduce_while({:ok, []}, fn scope, {:ok, acc} ->
      case query_requests(scope) do
        {:ok, requests} -> {:cont, {:ok, acc ++ requests}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp query_requests(%{query: query, roots: roots} = scope) do
    domain = Map.get(query, :domain) || %{}
    root = Enum.find_value(roots, &root_schema/1)
    declared = collect_declared(domain, %{})

    case Enum.find(declared, &match?({_table, :conflict}, &1)) do
      {table, :conflict} ->
        two_schemas([table])

      nil ->
        scope
        |> Selecto.Executor.scope_relations()
        |> Enum.uniq()
        |> Enum.map(fn table ->
          {table, declared[table] || if(String.contains?(table, "."), do: nil, else: root)}
        end)
        |> then(&{:ok, &1})
    end
  end

  defp root_schema(query) do
    source = (Map.get(query, :domain) || %{}) |> Map.get(:source) || %{}
    schema_name(Map.get(source, :source_schema) || Map.get(source, "source_schema"))
  end

  defp schema_name(schema) when is_binary(schema) and schema != "", do: schema
  defp schema_name(schema) when is_atom(schema) and not is_nil(schema), do: Atom.to_string(schema)
  defp schema_name(_schema), do: nil

  defp collect_declared(map, acc) when is_map(map) and not is_struct(map) do
    table = Map.get(map, :source_table) || Map.get(map, "source_table")
    schema = map |> Map.get(:source_schema, Map.get(map, "source_schema")) |> schema_name()

    acc =
      if table && schema do
        Map.update(acc, to_string(table), schema, fn
          ^schema -> schema
          _other -> :conflict
        end)
      else
        acc
      end

    Enum.reduce(Map.values(map), acc, &collect_declared/2)
  end

  defp collect_declared(list, acc) when is_list(list),
    do: Enum.reduce(list, acc, &collect_declared/2)

  defp collect_declared(_value, acc), do: acc

  defp two_schemas(tables),
    do:
      refusal(
        :relation_ambiguous,
        "reads one relation name in two schemas in one statement; " <>
          "declare the same schema for it wherever the statement names it",
        tables
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

  @doc """
  Rewrites the relation names a generated statement names after FROM, JOIN
  or INTO (quoted or bare, not already qualified, not a function call) that
  `map` (relations/1) declares a schema for to `"schema".name`, `{:ok, sql}`.

  CTE names are scoped (R33): in a leading non-recursive WITH, a CTE's own
  body and the bodies before it see the real relation of that name (a
  query member named like the relation it reads), the bodies after it and
  the main query see the CTE. A CTE name equal to a relation of `map` that
  this cannot scope (WITH RECURSIVE, a nested WITH) refuses.
  """
  def qualify(sql, map) do
    sql = IO.iodata_to_binary(sql)

    cond do
      map_size(map) == 0 -> {:ok, sql}
      scan_ctes(sql) == :unsure -> ctes_unresolved()
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
        quote_identifier(Map.fetch!(map, ref.name)) <>
        "." <>
        binary_part(acc, ref.start, byte_size(acc) - ref.start)
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
    sql
    |> Scanner.tokenize()
    |> Enum.map(fn
      {:word, text, _start} -> {:id, text}
      {:qident, text, _start} -> {:qid, text}
      {kind, text, _start} -> {kind, text}
    end)
    |> walk_ctes(nil, [])
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
      names = [fold_name(elem(name, 1)) | names]

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

  defp quote_identifier(identifier),
    do: "\"" <> String.replace(to_string(identifier), "\"", "\"\"") <> "\""
end
