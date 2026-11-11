defmodule SelectoDBPostgreSQL.DeclaredSchema.Scanner do
  @moduledoc false

  # Relation positions of a statement (R42): every item of a FROM list
  # (comma-separated, at any depth: subqueries, scalar subqueries, derived
  # tables), every JOIN target, INSERT/MERGE INTO, UPDATE, DELETE ... USING
  # and MERGE ... USING. FROM inside a function call (EXTRACT, SUBSTRING,
  # TRIM, OVERLAY) and IS DISTINCT FROM are not relation positions. String
  # literals, dollar-quoted strings, quoted identifiers and comments are
  # skipped. Each reference is `%{start, length, name, qualified?}`: `start`
  # and `length` cover the whole (possibly dotted) name, `name` is its last
  # part unquoted.

  @statement ~w(select update delete insert merge with values)
  @ends ~w(where group having order limit offset window union intersect except returning set fetch for)

  @doc "The statement's tokens: {kind, text, start}."
  def tokenize(sql), do: sql |> IO.iodata_to_binary() |> tokens(0, [])

  def refs(sql) do
    sql
    |> IO.iodata_to_binary()
    |> tokens(0, [])
    |> walk([frame(true)], [])
  end

  defp frame(statement?), do: %{statement?: statement?, items?: false, expect: false, prev: nil}

  # Tokens: {kind, text, start}, kind :word | :qident | :punct | :other.

  defp tokens(sql, i, acc) when i >= byte_size(sql), do: Enum.reverse(acc)

  # By the first byte: this runs on every statement, and a regex per token
  # costs more than the rest of the read.
  defp tokens(sql, i, acc) do
    rest = binary_part(sql, i, byte_size(sql) - i)

    case rest do
      <<c, _::binary>> when c in [?\s, ?\t, ?\n, ?\r, ?\f, ?\v] ->
        tokens(sql, i + 1, acc)

      <<"--", _::binary>> ->
        tokens(sql, i + span_to(rest, "\n", 1), acc)

      <<"/*", _::binary>> ->
        tokens(sql, i + span_to(rest, "*/", 2), acc)

      <<c, _::binary>> when c in [?(, ?), ?,, ?., ?;] ->
        tokens(sql, i + 1, [{:punct, <<c>>, i} | acc])

      <<c, q, tail::binary>> when c in [?E, ?e] and q == ?' ->
        quoted(sql, i, tail, 2, ?', :other, acc, rest)

      <<?', tail::binary>> ->
        quoted(sql, i, tail, 1, ?', :other, acc, rest)

      <<?", tail::binary>> ->
        quoted(sql, i, tail, 1, ?", :qident, acc, rest)

      <<c, _::binary>> when c in ?A..?Z or c in ?a..?z or c == ?_ or c >= 0x80 ->
        size = word_size(rest, 0)
        tokens(sql, i + size, [{:word, binary_part(rest, 0, size), i} | acc])

      _other ->
        other(sql, i, rest, acc)
    end
  end

  defp word_size(<<c, tail::binary>>, size)
       when c in ?A..?Z or c in ?a..?z or c in ?0..?9 or c == ?_ or c == ?$ or c >= 0x80,
       do: word_size(tail, size + 1)

  defp word_size(_rest, size), do: size

  # A quoted run with its doubled quotes; unterminated, what follows reads as
  # any other text.
  defp quoted(sql, i, tail, opening, quote, kind, acc, rest) do
    case quoted_size(tail, quote, opening) do
      nil ->
        other(sql, i, rest, acc)

      size ->
        tokens(sql, i + size, [{kind, binary_part(rest, 0, size), i} | acc])
    end
  end

  defp quoted_size(<<q, q, tail::binary>>, q, size), do: quoted_size(tail, q, size + 2)
  defp quoted_size(<<q, _::binary>>, q, size), do: size + 1
  defp quoted_size(<<_, tail::binary>>, q, size), do: quoted_size(tail, q, size + 1)
  defp quoted_size(<<>>, _q, _size), do: nil

  defp other(sql, i, rest, acc) do
    cond do
      match = Regex.run(~r/\A\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/, rest) ->
        tag = hd(match)
        tail = binary_part(rest, byte_size(tag), byte_size(rest) - byte_size(tag))

        len =
          case :binary.match(tail, tag) do
            {pos, _} -> byte_size(tag) * 2 + pos
            :nomatch -> byte_size(rest)
          end

        tokens(sql, i + len, [{:other, binary_part(rest, 0, len), i} | acc])

      match =
          Regex.run(
            ~r/\A(?:\$[0-9]+|[0-9][0-9.]*(?:[eE][+-]?[0-9]+)?|::|[^\sA-Za-z0-9_"'(),.;$]+|.)/s,
            rest
          ) ->
        tokens(sql, i + byte_size(hd(match)), [{:other, hd(match), i} | acc])
    end
  end

  defp span_to(rest, terminator, extra) do
    case :binary.match(rest, terminator) do
      {pos, _} -> pos + extra
      :nomatch -> byte_size(rest)
    end
  end

  # Walk: one frame per parenthesis depth. `statement?` marks a depth that
  # holds a statement (the top, or a subquery); `items?` a FROM/JOIN/USING
  # list in progress; `expect` that the next token starts an item.

  defp walk([], _stack, acc), do: Enum.reverse(acc)

  defp walk([{:punct, "(", _} | rest], [top | below], acc) do
    # A derived table or subquery item, or any other parenthesis.
    walk(rest, [frame(false), %{top | expect: false} | below], acc)
  end

  defp walk([{:punct, ")", _} | rest], [_top], acc), do: walk(rest, [frame(true)], acc)
  defp walk([{:punct, ")", _} | rest], [_top | below], acc), do: walk(rest, below, acc)

  defp walk([{:punct, ",", _} | rest], [top | below], acc) do
    top = if top.items?, do: %{top | expect: true}, else: top
    walk(rest, [top | below], acc)
  end

  defp walk([{:punct, ";", _} | rest], _stack, acc), do: walk(rest, [frame(true)], acc)

  defp walk([{kind, text, _start} | rest] = tokens, [top | below], acc)
       when kind in [:word, :qident] do
    word = if kind == :word, do: String.downcase(text), else: nil
    prev = top.prev
    top = %{top | prev: word}

    cond do
      top.expect != false and word in ["lateral", "only"] ->
        walk(rest, [top | below], acc)

      top.expect != false and word == "set" ->
        walk(rest, [%{top | items?: false, expect: false} | below], acc)

      # INSERT INTO t (cols): the parenthesis is a column list.
      top.expect == true and match?([{:punct, "(", _} | _], rest) ->
        # A function in FROM (unnest(...), generate_series(...)).
        walk(rest, [%{top | expect: false} | below], acc)

      top.expect != false ->
        {ref, rest} = chain(tokens)
        walk(rest, [%{top | expect: false, prev: nil} | below], [ref | acc])

      word == "update" ->
        # UPDATE target, but not FOR UPDATE, DO UPDATE, THEN UPDATE.
        expect = if prev in ~w(for do then key no share), do: false, else: :target
        walk(rest, [%{top | statement?: true, items?: false, expect: expect} | below], acc)

      word in @statement ->
        walk(rest, [%{top | statement?: true, items?: false, expect: false} | below], acc)

      top.statement? and word == "from" and prev != "distinct" ->
        walk(rest, [%{top | items?: true, expect: true} | below], acc)

      top.statement? and word == "join" ->
        walk(rest, [%{top | items?: true, expect: true} | below], acc)

      top.statement? and word == "into" ->
        walk(rest, [%{top | items?: false, expect: :target} | below], acc)

      top.statement? and word == "using" ->
        if match?([{:punct, "(", _} | _], rest),
          do: walk(rest, [top | below], acc),
          else: walk(rest, [%{top | items?: true, expect: true} | below], acc)

      top.statement? and word == "on" ->
        walk(rest, [%{top | expect: false} | below], acc)

      top.statement? and word in @ends ->
        walk(rest, [%{top | items?: false, expect: false} | below], acc)

      true ->
        walk(rest, [top | below], acc)
    end
  end

  # Anything else where an item was expected ($1, a literal) is no relation.
  defp walk([_token | rest], [%{expect: expect} = top | below], acc) when expect != false,
    do: walk(rest, [%{top | expect: false} | below], acc)

  defp walk([_token | rest], stack, acc), do: walk(rest, stack, acc)

  defp chain([{_kind, _text, start} | _] = tokens) do
    {parts, rest} = take_chain(tokens, [])
    {_, last_text, last_start} = List.last(parts)

    {%{
       start: start,
       length: last_start + byte_size(last_text) - start,
       name: unquote_name(last_text),
       folded: fold_name(last_text),
       qualified?: length(parts) > 1
     }, rest}
  end

  defp take_chain([{kind, _, _} = part, {:punct, ".", _}, {kind2, _, _} = next | rest], acc)
       when kind in [:word, :qident] and kind2 in [:word, :qident],
       do: take_chain([next | rest], [part | acc])

  defp take_chain([part | rest], acc), do: {Enum.reverse([part | acc]), rest}

  defp fold_name("\"" <> _ = name), do: unquote_name(name)
  defp fold_name(name), do: String.downcase(name)

  defp unquote_name("\"" <> _ = name),
    do: name |> String.slice(1..-2//1) |> String.replace("\"\"", "\"")

  defp unquote_name(name), do: name
end
