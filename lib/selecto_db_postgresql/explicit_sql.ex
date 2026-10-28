defmodule SelectoDBPostgreSQL.ExplicitSQL do
  @moduledoc false

  # R46 FINAL-2 / R47: the last step before a Selecto statement reaches
  # PostgreSQL. Every function it calls without a schema is qualified with
  # the schema PostgreSQL would choose under the pinned search_path, so a
  # same-named function in another schema with a closer argument match
  # (public.lower(varchar)) can never be chosen: pg_catalog's when it has
  # one, else the first pinned schema that defines it (an extension's own
  # schema). The statement is then refused (`bare_operator`) if a
  # comparison or pattern operator is still written bare (outside
  # OPERATOR(...)) or a function stays unqualified: no Selecto statement
  # runs by name-based operator or function lookup. Builders that still
  # write plain comparisons (tagging, hierarchy, OLAP, enhanced joins,
  # subfilters) are refused on PostgreSQL this way.

  alias SelectoDBPostgreSQL.RelationPath.Scanner
  alias Selecto.Write.Error

  # Words followed by "(" that are SQL syntax, not function calls.
  @syntax ~w(select from where and or not in exists any all some values array row cast coalesce
             nullif greatest least extract overlay position substring trim over filter within
             partition using on join lateral union intersect except as into table with recursive
             when then else case end by group order having limit offset fetch for distinct only
             rows range groups unnest_ordinality grouping sets cube rollup set returning conflict
             do update insert delete merge matched default interval operator like ilike between
             is null true false collate escape first last asc desc nulls window ordinality
             materialized lock share nowait skip locked of key primary references check
             constraint unique foreign xmlelement xmlforest xmlattributes json_object json_array
             json_arrayagg json_objectagg normalize treat convert merge_action current_date
             current_time current_timestamp localtime localtimestamp)

  @comparison ~w(= <> != < > <= >= ~~ !~~ ~~* !~~* ~ !~ ~* !~*)
  @comparison_words ~w(like ilike in between similar)

  @lookup_sql """
  SELECT r.name AS selecto_guard_function_name,
         (SELECT CAST(n.nspname AS pg_catalog.text)
          FROM pg_catalog.pg_proc p
          JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) p.pronamespace
          JOIN pg_catalog.unnest(pg_catalog.current_schemas(true)) WITH ORDINALITY AS s(nsp, ord)
            ON s.nsp OPERATOR(pg_catalog.=) n.nspname
          WHERE p.proname OPERATOR(pg_catalog.=) r.name
            AND n.oid OPERATOR(pg_catalog.<>) pg_catalog.pg_my_temp_schema()
          ORDER BY (n.oid OPERATOR(pg_catalog.=) CAST(11 AS pg_catalog.oid)) DESC, s.ord
          LIMIT 1) AS selecto_guard_function_schema
  FROM pg_catalog.unnest($1::pg_catalog.text[]) AS r(name)
  """

  @doc """
  `{:ok, sql}` with every unqualified function call qualified (one lookup
  through `execute` when there are any), or a typed refusal.
  """
  def prepare(execute, sql) do
    sql = sql |> IO.iodata_to_binary() |> explicit_simple_comparisons()
    calls = function_calls(sql)

    with {:ok, sql} <- qualify(execute, sql, calls) do
      case violations(sql) do
        [] -> {:ok, sql}
        found -> refuse(found)
      end
    end
  end

  @doc false
  # A bare comparison whose two operands are atoms (a dotted identifier, a
  # parameter, a number, a string literal, TRUE/FALSE) has exactly one
  # reading, so it is rewritten to pg_catalog's operator: the joins and
  # depth bounds of recursive query members, a retarget's context
  # correlation, built before the read's types are known. Anything else
  # stays bare and is refused.
  def explicit_simple_comparisons(sql) do
    tokens = sql |> Scanner.tokenize() |> List.to_tuple()
    count = tuple_size(tokens)
    assign = assignment_indexes(tokens)

    edits =
      for i <- 0..(count - 1)//1,
          {:other, op, start} <- [elem(tokens, i)],
          op in ~w(= <> != < > <= >=),
          not MapSet.member?(assign, i),
          left_atom?(tokens, i - 1),
          right_atom?(tokens, i + 1),
          do: {start, byte_size(op), pg_operator(op)}

    edits
    |> Enum.reverse()
    |> Enum.reduce(sql, fn {start, length, op}, acc ->
      binary_part(acc, 0, start) <>
        "OPERATOR(pg_catalog." <>
        op <>
        ")" <>
        binary_part(acc, start + length, byte_size(acc) - start - length)
    end)
  end

  defp pg_operator("!="), do: "<>"
  defp pg_operator(op), do: op

  @tighter ~w(+ - * / % ^ || :: [ ])

  defp atom_token?({:word, word, _}),
    do:
      String.downcase(word) not in (@syntax -- ~w(true false null)) or
        String.downcase(word) in ~w(true false)

  defp atom_token?({:qident, _, _}), do: true

  defp atom_token?({:other, text, _}),
    do: Regex.match?(~r/\A(\$[0-9]+|[0-9][0-9.]*|'.*')\z/s, text)

  defp atom_token?(_token), do: false

  # The operand before index i ends at i: an atom chain (a.b."c") not
  # itself preceded by a tighter-binding operator or a dot.
  defp left_atom?(_tokens, i) when i < 0, do: false

  defp left_atom?(tokens, i) do
    with true <- atom_token?(elem(tokens, i)),
         first when is_integer(first) <- chain_start(tokens, i) do
      first == 0 or not tighter?(elem(tokens, first - 1))
    else
      _ -> false
    end
  end

  defp chain_start(tokens, i) do
    if i >= 2 and match?({:punct, ".", _}, elem(tokens, i - 1)) and
         atom_token?(elem(tokens, i - 2)),
       do: chain_start(tokens, i - 2),
       else: i
  end

  defp right_atom?(tokens, i) when i >= tuple_size(tokens), do: false

  defp right_atom?(tokens, i) do
    with true <- atom_token?(elem(tokens, i)) do
      last = chain_end(tokens, i)

      last + 1 >= tuple_size(tokens) or
        not (tighter?(elem(tokens, last + 1)) or match?({:punct, "(", _}, elem(tokens, last + 1)))
    end
  end

  defp chain_end(tokens, i) do
    if i + 2 < tuple_size(tokens) and match?({:punct, ".", _}, elem(tokens, i + 1)) and
         atom_token?(elem(tokens, i + 2)),
       do: chain_end(tokens, i + 2),
       else: i
  end

  defp tighter?({:other, text, _}), do: text in @tighter
  defp tighter?({:punct, ".", _}), do: true
  defp tighter?(_), do: false

  # Token indexes of "=" inside SET assignment lists.
  defp assignment_indexes(tokens) do
    {set, _mode} =
      Enum.reduce(0..(tuple_size(tokens) - 1)//1, {MapSet.new(), nil}, fn i, {set, mode} ->
        case elem(tokens, i) do
          {:word, word, _} ->
            lower = String.downcase(word)

            cond do
              lower == "set" -> {set, :assign}
              lower in ~w(where from returning when) -> {set, nil}
              true -> {set, mode}
            end

          {:other, "=", _} when mode == :assign ->
            {MapSet.put(set, i), mode}

          _ ->
            {set, mode}
        end
      end)

    set
  end

  @doc "Bare comparison operators and unqualified function calls left in `sql`."
  def violations(sql) do
    sql = IO.iodata_to_binary(sql)

    Enum.map(bare_operators(sql), &{:operator, &1}) ++
      Enum.map(function_calls(sql), &{:function, &1.name})
  end

  defp qualify(_execute, sql, []), do: {:ok, sql}

  defp qualify(execute, sql, calls) do
    names = calls |> Enum.map(&String.downcase(&1.name)) |> Enum.uniq()

    case execute.(@lookup_sql, [names]) do
      {:ok, %{rows: rows}} when is_list(rows) ->
        schemas = Map.new(rows, fn [name, schema] -> {name, schema} end)

        {:ok,
         calls
         |> Enum.reverse()
         |> Enum.reduce(sql, fn call, acc ->
           case Map.get(schemas, String.downcase(call.name)) do
             nil ->
               acc

             schema ->
               prefix =
                 if schema == "pg_catalog",
                   do: "pg_catalog.",
                   else: quote_identifier(schema) <> "."

               binary_part(acc, 0, call.start) <>
                 prefix <> binary_part(acc, call.start, byte_size(acc) - call.start)
           end
         end)}

      _ ->
        refuse([:function_lookup_unavailable])
    end
  end

  @doc false
  # Unqualified function calls: a bare word followed by "(", not SQL
  # syntax, not after "." (qualified), AS/INTO/TABLE (a column list or
  # type), and not a CTE's column list (name (cols) AS ( ... )).
  def function_calls(sql) do
    tokens = sql |> Scanner.tokenize() |> List.to_tuple()
    count = tuple_size(tokens)

    for i <- 0..(count - 1)//1,
        {:word, text, start} <- [elem(tokens, i)],
        i + 1 < count,
        match?({:punct, "(", _}, elem(tokens, i + 1)),
        String.downcase(text) not in @syntax,
        not after?(tokens, i, fn
          {:punct, ".", _} -> true
          {:word, word, _} -> String.downcase(word) in ~w(as into table)
          _ -> false
        end),
        not column_list?(tokens, i + 1),
        do: %{name: text, start: start}
  end

  @doc false
  # Comparison and pattern operators written bare: outside OPERATOR(...),
  # outside SET assignment lists and POSITION(a IN b).
  def bare_operators(sql) do
    sql |> Scanner.tokenize() |> scan_operators(%{mode: nil, depth: 0, skip: []}, [])
  end

  defp scan_operators([], _state, acc), do: Enum.reverse(acc)

  # OPERATOR(schema.op): skipped whole.
  defp scan_operators([{:word, word, _}, {:punct, "(", _} | rest], state, acc)
       when word in ["OPERATOR", "operator", "Operator"] do
    scan_operators(drop_group(rest, 1), state, acc)
  end

  # POSITION(a IN b): IN is syntax.
  defp scan_operators([{:word, word, _}, {:punct, "(", _} | rest], state, acc)
       when word in ["POSITION", "position"] do
    scan_operators(drop_group(rest, 1), state, acc)
  end

  defp scan_operators([{:punct, "(", _} | rest], state, acc),
    do:
      scan_operators(
        rest,
        %{state | depth: state.depth + 1, skip: [state.mode | state.skip]},
        acc
      )

  defp scan_operators([{:punct, ")", _} | rest], state, acc) do
    {mode, skip} =
      case state.skip do
        [mode | skip] -> {mode, skip}
        [] -> {nil, []}
      end

    scan_operators(rest, %{state | depth: max(state.depth - 1, 0), mode: mode, skip: skip}, acc)
  end

  defp scan_operators([{:word, word, _} | rest], state, acc) do
    lower = String.downcase(word)

    cond do
      lower == "set" ->
        scan_operators(rest, %{state | mode: :assign}, acc)

      # LOCK TABLE t IN ... MODE: IN is syntax.
      lower == "lock" ->
        scan_operators(rest, %{state | mode: :lock}, acc)

      lower == "in" and state.mode == :lock ->
        scan_operators(rest, state, acc)

      lower in ~w(where from returning on when) ->
        scan_operators(rest, %{state | mode: nil}, acc)

      lower == "is" and distinct_from?(rest) ->
        scan_operators(rest, state, ["IS DISTINCT FROM" | acc])

      lower in @comparison_words ->
        scan_operators(rest, state, [word | acc])

      true ->
        scan_operators(rest, state, acc)
    end
  end

  defp scan_operators([{:other, text, _} | rest], state, acc) do
    if text in @comparison and not (state.mode == :assign and text == "="),
      do: scan_operators(rest, state, [text | acc]),
      else: scan_operators(rest, state, acc)
  end

  defp scan_operators([_token | rest], state, acc), do: scan_operators(rest, state, acc)

  # IS [NOT] DISTINCT FROM compares with "=" by name.
  defp distinct_from?([{:word, a, _}, {:word, b, _} | _]),
    do:
      String.downcase(a) == "distinct" or
        (String.downcase(a) == "not" and String.downcase(b) == "distinct")

  defp distinct_from?(_rest), do: false

  defp drop_group(tokens, 0), do: tokens
  defp drop_group([], _depth), do: []
  defp drop_group([{:punct, "(", _} | rest], depth), do: drop_group(rest, depth + 1)
  defp drop_group([{:punct, ")", _} | rest], depth), do: drop_group(rest, depth - 1)
  defp drop_group([_ | rest], depth), do: drop_group(rest, depth)

  defp after?(_tokens, 0, _fun), do: false
  defp after?(tokens, i, fun), do: fun.(elem(tokens, i - 1))

  # name ( ... ) AS ( : a CTE's column list.
  defp column_list?(tokens, open) do
    case close_index(tokens, open + 1, 1) do
      nil ->
        false

      close ->
        close + 2 < tuple_size(tokens) and
          match?({:word, _, _}, elem(tokens, close + 1)) and
          String.downcase(elem(elem(tokens, close + 1), 1)) == "as" and
          match?({:punct, "(", _}, elem(tokens, close + 2))
    end
  end

  defp close_index(tokens, i, _depth) when i >= tuple_size(tokens), do: nil

  defp close_index(tokens, i, depth) do
    case elem(tokens, i) do
      {:punct, "(", _} -> close_index(tokens, i + 1, depth + 1)
      {:punct, ")", _} when depth == 1 -> i
      {:punct, ")", _} -> close_index(tokens, i + 1, depth - 1)
      _ -> close_index(tokens, i + 1, depth)
    end
  end

  defp quote_identifier(name), do: "\"" <> String.replace(name, "\"", "\"\"") <> "\""

  defp refuse(found),
    do:
      {:error,
       Error.new(
         :query_enforcement,
         "PostgreSQL refuses a statement that would resolve an operator or function by name",
         details: %{
           code: :query_rule_not_evaluable,
           reason: :bare_operator,
           found: found |> Enum.take(5) |> inspect(),
           adapter: :postgresql
         }
       )}
end
